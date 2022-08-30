package elasticsearchutil

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	uuid "github.com/kthomas/go.uuid"
	"github.com/olivere/elastic/v7"
)

// Indexer instances buffer bulk indexing transactions
type Indexer struct {
	client           *elastic.Client
	identifier       string
	esBulkService    *elastic.BulkService
	flushMutex       *sync.Mutex
	q                chan *Message
	queueFlushTimer  *time.Timer
	queueSizeInBytes int

	shutdown chan bool
}

// Message is injested by indexer, routing `payload` to the elasticsearch index specified in `header`
type Message struct {
	Header  *MessageHeader `json:"header,omitempty"`
	Payload []byte         `json:"payload"`
}

// MessageHeader allows metadata about the payload to be provided; this metadata contains parameters related to elasticsearch
type MessageHeader struct {
	DocType *string `json:"doc_type,omitempty"`
	ID      *string `json:"id,omitempty"`
	Index   *string `json:"index,omitempty"`
}

// NewIndexer convenience method to initialize a new in-memory `Indexer` instance
func NewIndexer() (indexer *Indexer) {
	indexer = new(Indexer)

	instanceID, _ := uuid.NewV4()
	indexer.identifier = base64.RawURLEncoding.EncodeToString(instanceID.Bytes())

	indexer.client, _ = GetClient()
	indexer.flushMutex = &sync.Mutex{}
	indexer.q = make(chan *Message)

	indexer.queueSizeInBytes = 0
	indexer.queueFlushTimer = time.NewTimer(time.Second * time.Duration(elasticMaxBatchInterval))
	indexer.queueFlushTimer.Stop()

	indexer.setupBulkIndexer()

	return indexer
}

// Run the indexer instance
func (indexer *Indexer) Run() error {
	log.Infof("running elasticsearch indexer instance %v", indexer.identifier)

	for {
		select {
		case msg, ok := <-indexer.q:
			if ok {
				log.Debugf("received %d-byte delivery on inbound channel for indexer: %s", len(msg.Payload), indexer.identifier)

				if msg.Header.DocType != nil && msg.Header.Index != nil {
					log.Debugf("attempting to index %d-byte %s document delivered for index %s", len(msg.Payload), *msg.Header.DocType, *msg.Header.Index)
					indexer.index(msg)
				} else {
					log.Warningf("skipped indexing %d-byte document delivered with invalid headers", len(msg.Payload))
					// this is an implicit rejection of the delivery
				}
			} else {
				log.Debug("closed consumer channel")
				return nil
			}

		case t := <-indexer.queueFlushTimer.C:
			log.Debugf("indexer (%v) queue flush timer invoked at %v", indexer.identifier, t)
			indexer.esBulkServiceFlush()

		case <-indexer.shutdown:
			log.Debugf("closing indexer (%v) on Shutdown", indexer.identifier)
			indexer.cleanup()
			indexer.esBulkServiceFlush()
			return nil
		}
	}
}

// Stop the indexer instance
func (indexer *Indexer) Stop() {
	indexer.shutdown <- true
}

// Q enqueues the given message for inclusion in the bulk indexing process
func (indexer *Indexer) Q(msg *Message) error {
	return indexer.index(msg)
}

func (indexer *Indexer) cleanup() {
	log.Debugf("cleaning up indexer (%v)", indexer.identifier)
	indexer.queueFlushTimer.Stop()

	log.Debugf("closing buffered queue for indexer (%v)", indexer.identifier)
	close(indexer.q)

	log.Infof("indexer instance (%v) closed", indexer.identifier)
}

func (indexer *Indexer) setupBulkIndexer() error {
	indexer.esBulkService = elastic.NewBulkService(indexer.client)
	indexer.esBulkService.Timeout(fmt.Sprintf("%ds", elasticTimeout))
	indexer.esBulkService.Pretty(false)

	return nil
}

func (indexer *Indexer) index(msg *Message) error {
	if indexer.queueSizeInBytes == 0 {
		log.Debugf("indexer (%v) queue is currently empty, resetting queue flush timer", indexer.identifier)
		indexer.queueFlushTimer.Reset(time.Second * time.Duration(elasticMaxBatchInterval))
	}

	if msg.Header == nil {
		return fmt.Errorf("failed to index %d-byte message; no header provided", len(msg.Payload))
	}

	if msg.Header.DocType == nil {
		return fmt.Errorf("failed to index %d-byte message; no doc_type provided in header", len(msg.Payload))
	}

	if msg.Header.Index == nil {
		return fmt.Errorf("failed to index %d-byte message; no index provided in header", len(msg.Payload))
	}

	size := len(msg.Payload)
	docType := msg.Header.DocType
	index := msg.Header.Index
	log.Debugf("attempting to index %d-byte %v document in index %v: %v", size, docType, index, msg)

	log.Debugf("current size of indexer (%v) queue size in bytes: %d", indexer.identifier, indexer.queueSizeInBytes)
	if indexer.queueSizeInBytes+size >= elasticMaxBatchSizeBytes {
		log.Debugf("adding %d-byte %v document would exceed configured max %d-byte batch size", size, docType, elasticMaxBatchSizeBytes)
		indexer.esBulkServiceFlush()
	}

	req := elastic.NewBulkIndexRequest().Index(*index).Type(*docType).Doc(string(msg.Payload))
	if msg.Header.ID != nil {
		req.Id(*msg.Header.ID)
	}

	log.Debugf("queueing request in elasticsearch bulk index service: %v", req.String())
	indexer.esBulkService.Add(req)
	indexer.queueSizeInBytes += size

	return nil
}

func (indexer *Indexer) esBulkServiceFlush() (*elastic.BulkResponse, error) {
	indexer.flushMutex.Lock()
	defer indexer.flushMutex.Unlock()

	indexer.queueSizeInBytes = 0
	if indexer.esBulkService.NumberOfActions() == 0 {
		msg := fmt.Sprintf("indexer (%v) attempted to send Elasticsearch bulk index request, but nothing was queued", indexer.identifier)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	response, err := indexer.esBulkService.Do(context.TODO())
	if err != nil {
		log.Warningf("elasticsearch bulk index request failed: %v", err)
		// FIXME-- implement strategy to retry failed items
		// in some cases, we will want to requeue the reconstituted message (i.e. ES connection timeout)...
		// and in other cases, we will want to reject the message and not requeue it (i.e. bad request).
	} else {
		log.Infof("elasticsearch bulk index of %d items succeeded in %d ms", len(response.Items), response.Took)
		log.Debugf("elasticsearch bulk index response items: %v", response.Items)

		for _, item := range response.Succeeded() {
			messageId := item.Id
			docType := item.Type
			log.Debugf("elasticsearch bulk indexer indexed %v document with id: %v", docType, messageId)
		}

		for _, item := range response.Failed() {
			log.Debugf("elasticsearch bulk indexer document failed indexing: %v", item.Error)
		}
	}

	return response, err
}
