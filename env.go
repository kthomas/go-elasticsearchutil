package elasticsearchutil

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	logger "github.com/kthomas/go-logger"
	"gopkg.in/olivere/elastic.v6"
)

const defaultElasticsearchPort = 9200

var (
	// elasticClients is an array of configured elasticsearch clients
	elasticClients []*elastic.Client

	// elasticHosts is an array of <host>:<port> strings
	elasticHosts []string

	// The elasticsearch timeout
	elasticTimeout uint

	// The maximum batch size in bytes for a single elasticsearch bulk index request
	elasticMaxBatchSizeBytes int

	// The maximum interval in milliseconds between elasticsearch bulk index requests
	elasticMaxBatchInterval int

	log *logger.Logger
)

func init() {
	log = logger.NewLogger("elasticsearchutil", getLogLevel(), getSyslogEndpoint())
}

func getLogLevel() string {
	lvl := os.Getenv("ELASTICSEARCH_LOG_LEVEL")
	if lvl == "" {
		lvl = "info"
	}
	return lvl
}

func getSyslogEndpoint() *string {
	var endpoint *string
	if os.Getenv("SYSLOG_ENDPOINT") != "" {
		endpoint = stringOrNil(os.Getenv("SYSLOG_ENDPOINT"))
	}
	return endpoint
}

func requireElasticsearchConn() {
	elasticClients = make([]*elastic.Client, 0)

	for _, host := range elasticHosts {
		port := defaultElasticsearchPort
		hostparts := strings.Split(host, ":")
		if len(hostparts) == 2 {
			parsedPort, err := strconv.Atoi(hostparts[1])
			if err != nil {
				log.Panicf("invalid port parsed during elasticsearch client configuration; %s", err.Error())
			}
			port = parsedPort
		}

		elasticURL := fmt.Sprintf("http://%s:%d", host, port)
		client, err := elastic.NewClient(
			elastic.SetURL(elasticURL),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(true),
		)

		if err != nil {
			log.Panicf("failed to open elasticsearch connection; %s", err.Error())
		}

		elasticClients = append(elasticClients, client)
	}

	log.Debugf("configured %d elasticsearch clients", len(elasticClients))
}
