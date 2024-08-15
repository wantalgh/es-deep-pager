package main

import (
	"fmt"
	"net/url"
	"strings"

	esdeeppager "wantalgh/es-deep-pager"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
)

func main() {

	transport := buildEsTransport()
	client := esdeeppager.DeepPageClient{
		Transport: transport,
	}
	list, _ := client.Search(
		"test_data_*",
		"{\"match_all\": {}}",
		&[]string{"*"},
		"id",
		true,
		100000000,
		10000,
	)
	fmt.Print(len(*list))
}

func buildEsTransport() *elastictransport.Client {

	schame := "http"
	hosts := "node1:9200,node2:9200,node3:9200"

	username := "elastic"
	password := "search"

	urls := make([]*url.URL, 0)

	for _, value := range strings.Split(hosts, ",") {
		nodeUrl, _ := url.Parse(schame + "://" + value)
		urls = append(urls, nodeUrl)
	}
	nodeConfig := elastictransport.Config{
		URLs:     urls,
		Username: username,
		Password: password,
	}

	transport, _ := elastictransport.New(nodeConfig)
	return transport
}
