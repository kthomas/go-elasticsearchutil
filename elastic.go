package elasticsearchutil

import (
	"errors"
	"fmt"
	"os"
	"strconv"
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

	if os.Getenv("ELASTICSEARCH_USERNAME") != "" {
		elasticUsername = stringOrNil(os.Getenv("ELASTICSEARCH_USERNAME"))
	}

	if os.Getenv("ELASTICSEARCH_PASSWORD") != "" {
		elasticPassword = stringOrNil(os.Getenv("ELASTICSEARCH_PASSWORD"))
	}

	requireElasticsearchConn()
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

		scheme := defaultElasticsearchScheme
		if port == 443 {
			scheme = "https"
		}

		elasticURL := fmt.Sprintf("%s://%s", scheme, host)
		if port != 80 && port != 443 {
			elasticURL = fmt.Sprintf("%s:%d", elasticURL, port)
		}

		var client *elastic.Client
		var err error

		basicAuthConfigured := elasticUsername != nil && elasticPassword != nil

		if !basicAuthConfigured {
			client, err = elastic.NewClient(
				elastic.SetURL(elasticURL),
				elastic.SetSniff(false),
				elastic.SetHealthcheck(true),
			)
		} else {
			client, err = elastic.NewClient(
				elastic.SetURL(elasticURL),
				elastic.SetSniff(false),
				elastic.SetHealthcheck(true),
				elastic.SetBasicAuth(*elasticUsername, *elasticPassword),
			)
		}

		if err != nil {
			log.Panicf("failed to open elasticsearch connection; %s", err.Error())
		}

		elasticClients = append(elasticClients, client)
	}

	log.Debugf("configured %d elasticsearch clients", len(elasticClients))
}
