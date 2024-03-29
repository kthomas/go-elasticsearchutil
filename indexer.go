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

const defaultElasticsearchIndexerBufferedChannelSize = 64
const defaultElasticsearchIndexerMaxBatchIntervalMillis = 10000
const defaultElasticsearchIndexerMaxBatchSizeBytes = 1024 * 10
const defaultElasticsearchIndexerSleepIntervalMillis = 1000

// Indexer instances buffer bulk indexing transactions
type Indexer struct {
	client           *elastic.Client
	identifier       string
	esBulkService    *elastic.BulkService
	flushMutex       *sync.Mutex
	q                chan *Message
	queueFlushTicker *time.Ticker
	queueSizeInBytes int
	sleepInterval    time.Duration

	shutdown chan bool
}

// Message is injested by indexer, routing `payload` to the elasticsearch index specified in `header`
type Message struct {
	Header  *MessageHeader `json:"header,omitempty"`
	Payload []byte         `json:"payload"`
}

// MessageHeader allows metadata about the payload to be provided; this metadata contains parameters related to elasticsearch
type MessageHeader struct {
	ID    *string `json:"id,omitempty"`
	Index *string `json:"index,omitempty"`
}

// NewIndexer convenience method to initialize a new in-memory `Indexer` instance
func NewIndexer() (indexer *Indexer) {
	indexer = new(Indexer)

	instanceID, _ := uuid.NewV4()
	indexer.identifier = base64.RawURLEncoding.EncodeToString(instanceID.Bytes())

	indexer.client, _ = GetClient()
	indexer.flushMutex = &sync.Mutex{}
	indexer.q = make(chan *Message, defaultElasticsearchIndexerBufferedChannelSize)

	indexer.queueSizeInBytes = 0
	indexer.sleepInterval = time.Millisecond * time.Duration(defaultElasticsearchIndexerSleepIntervalMillis)

	indexer.setupBulkIndexer()

	return indexer
}

// Run the indexer instance
func (indexer *Indexer) Run() error {
	log.Infof("running elasticsearch indexer instance %v", indexer.identifier)
	indexer.queueFlushTicker = time.NewTicker(time.Millisecond * time.Duration(defaultElasticsearchIndexerMaxBatchIntervalMillis))

	for {
		select {
		case msg, ok := <-indexer.q:
			if ok {
				log.Debugf("received %d-byte delivery on inbound channel for indexer: %s", len(msg.Payload), indexer.identifier)

				if msg.Header.Index != nil {
					log.Debugf("attempting to index %d-byte document delivered for index %s", len(msg.Payload), *msg.Header.Index)
					indexer.index(msg)
				} else {
					log.Warningf("skipped indexing %d-byte document delivered with invalid headers", len(msg.Payload))
					// this is an implicit rejection of the delivery
				}
			} else {
				log.Debug("closed consumer channel")
				// return nil
			}

		case t := <-indexer.queueFlushTicker.C:
			log.Tracef("indexer (%v) queue flush timer invoked at %v", indexer.identifier, t)
			indexer.esBulkServiceFlush()

		case <-indexer.shutdown:
			log.Debugf("shutting down indexer (%v)", indexer.identifier)
			indexer.cleanup()
			indexer.esBulkServiceFlush()
			return nil

		default:
			time.Sleep(indexer.sleepInterval)
		}
	}
}

// Stop the indexer instance
func (indexer *Indexer) Stop() {
	indexer.shutdown <- true
}

// Q enqueues the given message for inclusion in the bulk indexing process
func (indexer *Indexer) Q(msg *Message) error {
	indexer.q <- msg
	return nil
}

func (indexer *Indexer) cleanup() {
	log.Debugf("cleaning up indexer (%v)", indexer.identifier)
	indexer.queueFlushTicker.Stop()

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
		indexer.queueFlushTicker.Reset(time.Millisecond * time.Duration(defaultElasticsearchIndexerMaxBatchIntervalMillis))
	}

	if msg.Header == nil {
		return fmt.Errorf("failed to index %d-byte message; no header provided", len(msg.Payload))
	}

	if msg.Header.Index == nil {
		return fmt.Errorf("failed to index %d-byte message; no index provided in header", len(msg.Payload))
	}

	size := len(msg.Payload)
	index := msg.Header.Index

	log.Tracef("attempting to index %d-byte %v document in index %v: %v", size, index, msg)
	log.Tracef("current bulk queue size of indexer (%v) in bytes: %d", indexer.identifier, indexer.queueSizeInBytes)

	if indexer.queueSizeInBytes+size >= defaultElasticsearchIndexerMaxBatchSizeBytes {
		log.Debugf("adding %d-byte %v document would exceed configured max %d-byte batch size", size, defaultElasticsearchIndexerMaxBatchSizeBytes)
		indexer.esBulkServiceFlush()
	}

	req := elastic.NewBulkIndexRequest().Index(*index).Doc(string(msg.Payload))
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
		log.Tracef(msg)
		return nil, errors.New(msg)
	}

	response, err := indexer.esBulkService.Do(context.TODO())
	if err != nil {
		log.Warningf("elasticsearch bulk index request failed: %v", err)
		// FIXME-- implement strategy to retry failed items
		// in some cases, we will want to requeue the reconstituted message (i.e. ES connection timeout)...
		// and in other cases, we will want to reject the message and not requeue it (i.e. bad request).
	} else {
		log.Debugf("indexer (%v) successfully indexed %d items in %dms via bulk request", len(response.Items), response.Took)
		log.Tracef("elasticsearch bulk index response items: %v", response.Items)

		for _, item := range response.Succeeded() {
			messageId := item.Id
			docType := item.Type
			log.Tracef("indexer (%v) indexed %v document with id: %v", indexer.identifier, docType, messageId)
		}

		for _, item := range response.Failed() {
			log.Warningf("indexer (%v) failed to index document in bulk request; %v", item.Error)
		}
	}

	return response, err
}
