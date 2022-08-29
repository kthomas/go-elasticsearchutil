package elasticsearchutil

import (
	"errors"
	"os"
	"strings"

	"gopkg.in/olivere/elastic.v6"
)

// GetClient returns the first configured elasticsearch client
func GetClient() (*elastic.Client, error) {
	if len(elasticClients) == 0 {
		return nil, errors.New("failed to retrieve elasticsearch client")
	}

	return elasticClients[0], nil
}

// RequireElasticsearch reads the environment and initializes the configured elasticsearch client
func RequireElasticsearch() {
	elasticHosts = make([]string, 0)

	if os.Getenv("ELASTICSEARCH_HOSTS") != "" {
		hosts := strings.Split(os.Getenv("ELASTICSEARCH_HOSTS"), ",")
		for _, host := range hosts {
			elasticHosts = append(elasticHosts, strings.Trim(host, " "))
		}
	} else {
		log.Panicf("failed to parse ELASTICSEARCH_HOSTS from environment")
	}

	requireElasticsearchConn()
}
