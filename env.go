package elasticsearchutil

import (
	"os"

	logger "github.com/kthomas/go-logger"
	"github.com/olivere/elastic/v7"
)

const defaultElasticsearchPort = 9200
const defaultElasticsearchScheme = "http"

var (
	// elasticClients is an array of configured elasticsearch clients
	elasticClients []*elastic.Client

	// elasticHosts is an array of <host>:<port> strings
	elasticHosts []string

	// The elasticsearch timeout
	elasticTimeout uint

	// The API scheme, i.e., 'https', to force the elasticsearch client to use for new connections
	elasticAPIScheme *string

	// When true, self-signed certificates are accepted when connecting to elasticsearch via https
	elasticAcceptSelfSignedCertificate bool

	// The maximum batch size in bytes for a single elasticsearch bulk index request
	elasticMaxBatchSizeBytes int

	// The maximum interval in milliseconds between elasticsearch bulk index requests
	elasticMaxBatchInterval int

	// The username for basic authorization when communicating with elasticsearch
	elasticUsername *string

	// The password for basic authorization when communicating with elasticsearch
	elasticPassword *string

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
