package elasticsearchutil

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/olivere/elastic/v7"
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

	if os.Getenv("ELASTICSEARCH_API_SCHEME") != "" {
		elasticAPIScheme = stringOrNil(os.Getenv("ELASTICSEARCH_API_SCHEME"))
	}

	if os.Getenv("ELASTICSEARCH_ACCEPT_SELF_SIGNED_CERTIFICATE") != "" {
		elasticAcceptSelfSignedCertificate = strings.EqualFold(strings.ToLower(os.Getenv("ELASTICSEARCH_ACCEPT_SELF_SIGNED_CERTIFICATE")), "true")
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
		if elasticAPIScheme != nil {
			scheme = *elasticAPIScheme
		} else if port == 443 {
			scheme = "https"
		}

		elasticURL := fmt.Sprintf("%s://%s", scheme, hostparts[0])
		if port != 80 && port != 443 {
			elasticURL = fmt.Sprintf("%s:%d", elasticURL, port)
		}

		var client *elastic.Client
		var err error

		basicAuthConfigured := elasticUsername != nil && elasticPassword != nil

		httpClient := &http.Client{}
		if strings.EqualFold(scheme, "https") && elasticAcceptSelfSignedCertificate {
			httpClient.Transport = &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			}
		}

		if !basicAuthConfigured {
			client, err = elastic.NewClient(
				elastic.SetHttpClient(httpClient),
				elastic.SetURL(elasticURL),
				elastic.SetSniff(false),
				elastic.SetHealthcheck(true),
			)
		} else {
			client, err = elastic.NewClient(
				elastic.SetHttpClient(httpClient),
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
