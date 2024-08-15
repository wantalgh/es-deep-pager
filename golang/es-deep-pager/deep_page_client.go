/*
MIT License

Copyright (c) 2024 wantalgh

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package esdeeppager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
)

// DeepPageClient is a client for the Elasticsearch deep page search API.
//
// This client provides the search function of elasticsearch.
// Using this client to query data from the elasticsearch cluster, you can use very large "from" and "size" parameter values
// without changing the default max_result_window setting of the index.
//
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
type DeepPageClient struct {

	// elasticsearch low level transport client
	// Reference: https://github.com/elastic/elastic-transport-go
	Transport *elastictransport.Client
}

const (
	maxFrom = 2000
	maxSize = 3000
)

// Search method, receives parameters such as index, queryDsl, from, size, etc., and call the searchAPI
// of elasticsearch to query data.
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html
//
// The `index` parameter specifies the index name for query, can use wildcards, e.g. my-index-*.
// This will be placed in search request path.
// For lower versions of elasticsearch, it can contains type, e.g. my-index/my-type.
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
//
// The `query` parameter specifies the Query Dsl for query, this is a json formatted string, e.g. {"match_all":{}}.
// This will be placed in the "query" field of the request body.
// If not specified, the search will return all documents in the index.
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
//
// The `source` parameter specifies the source filter for query, e.g. ["column1", "column2", "obj1.*", "obj2.*" ].
// This will be placed in the "_source" field of the request body.
// If not specified, The query will return fields based on the default settings of the index.
// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html
//
// The `sort` parameter specifies the sort field for query, e.g. "id". This will be placed in the "sort" field of the request body.
// In order to implement fast large from parameter query, the queried data must be a well-ordered set.
// All the documents to be queried must have at least one unique number field, which is a numeric type and stores the
// unique number of each document. The available range of the number is the entire long integer, which can be negative
// and discontinuous, but the number of each document must not be repeated.
// When performing fast from query, a unique number field must be used as sorting.
//
// The `asc` parameter specifies the Sort order of the unique number field, if true, means ascending, if false, means descending.
//
// The `from` parameter specifies the Starting document offset, how many documents to skip. a non-negative number. e.g. 100000000
// Using this client, you can use very large from parameter without changing the default max_result_window setting of the index.

// The `size` parameter specifies the the number of hits to return. a non-negative number. e.g. 1000000
// Using this client, you can use large value parameter without changing the default max_result_window setting of the index.
//
// Returns a slice of maps containing the search results and an error if the search fails.
// If no documents match the query, an empty array is returned.
func (client *DeepPageClient) Search(index string, query string, source *[]string, sort string, asc bool, from int64, size int64) (*[]map[string]any, error) {

	if index == "" {
		return nil, errors.New("index must be specified")
	}
	if from < 0 || size < 0 {
		return nil, errors.New("from and size must be greater than 0")
	}
	if size == 0 {
		return &[]map[string]any{}, nil
	}
	if query == "" {
		query = "{\"match_all\":{}}"
	}

	// When the queried data is near the end of the data set, reverse the query direction.
	reverse := false
	if from > maxFrom {
		total, err := client.count(index, query)
		if err != nil {
			return nil, err
		}
		if total == 0 || from > total {
			return &[]map[string]any{}, nil
		}
		reverse = from > (total - from)
		if reverse {
			asc = !asc
			from2 := total - from - size
			size2 := size
			if from2 < 0 {
				size2 = size + from2
			}
			from = maximum(from2, 0)
			size = maximum(size2, 0)
			if size == 0 {
				return &[]map[string]any{}, nil
			}
		}
	}

	// When the from parameter is large, find a sort value that can exclude some of the from data, and reduce the from value.
	newQuery := query
	newFrom := from
	if from > maxFrom {
		minItem, err := client.query(index, query, &[]string{sort}, sort, true, 0, 1)
		if err != nil {
			return nil, err
		}

		sortMin := int64(((*minItem)[0])["_source"].(map[string]any)[sort].(float64))
		maxItem, err := client.query(index, query, &[]string{sort}, sort, false, 0, 1)
		if err != nil {
			return nil, err
		}
		sortMax := int64(((*maxItem)[0])["_source"].(map[string]any)[sort].(float64))

		if asc {
			newStart, newForm2, err := client.findNewFrom(index, query, sort, sortMin, sortMax, from)
			if err != nil {
				return nil, err
			}
			newFrom = newForm2
			newQuery = buildCmpQuery(query, sort, "gt", newStart)
		} else {
			newStart, newFrom2, err := client.findNewFrom(index, query, sort, sortMax, sortMin, from)
			if err != nil {
				return nil, err
			}
			newFrom = newFrom2
			newQuery = buildCmpQuery(query, sort, "lt", newStart)
		}
	}

	// When the size parameter is large, query data in batches to reduce the size value.
	remainSize := size
	retrieveSize := minimum(size, maxSize)
	batch, err := client.query(index, newQuery, source, sort, asc, newFrom, retrieveSize)
	if err != nil {
		return nil, err
	}
	if len(*batch) == 0 {
		return &[]map[string]any{}, nil
	}

	var list []map[string]any
	list = append(list, *batch...)
	remainSize -= int64(len(*batch))
	for remainSize > 0 {
		lastSort := int64(((*batch)[len(*batch)-1])["sort"].([]any)[0].(float64))
		if asc {
			newQuery = buildCmpQuery(query, sort, "gt", lastSort)
		} else {
			newQuery = buildCmpQuery(query, sort, "lt", lastSort)
		}
		retrieveSize = minimum(remainSize, maxSize)
		batch, err = client.query(index, newQuery, source, sort, asc, 0, retrieveSize)
		if err != nil {
			return nil, err
		}
		if len(*batch) == 0 {
			break
		}
		list = append(list, *batch...)
		remainSize -= int64(len(*batch))
	}

	// If result is reverse query data, reverse it back.
	if reverse {
		for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
			list[i], list[j] = list[j], list[i]
		}
	}

	return &list, nil
}

// iif returns truePart if expr is true, and falsePart otherwise.
func iif[T any](expr bool, truePart T, falsePart T) T {
	if expr {
		return truePart
	}
	return falsePart
}

// Returns the lesser of i1 and i2.
func maximum(i1, i2 int64) int64 {
	return iif(i1 > i2, i1, i2)
}

// Returns the greater of i1 and i2.
func minimum(i1, i2 int64) int64 {
	return iif(i1 < i2, i1, i2)
}

// Returns the distance of i1 and i2.
func distance(i1, i2 int64) int64 {
	return iif(i1 > i2, i1-i2, i2-i1)
}

// Use binary search to find new query parameters with the same result as the original query but with a smaller from value.
func (client *DeepPageClient) findNewFrom(index string, query string, sort string, sortStart int64, sortEnd int64, from int64) (int64, int64, error) {
	newStart := sortStart
	newEnd := sortEnd
	var newFrom int64
	for {
		sortMin := minimum(newStart, newEnd)
		sortAbs := distance(newStart, newEnd)
		if sortAbs <= 1 {
			return sortMin, sortAbs, nil
		}
		sortMid := sortMin + sortAbs/2
		var midQuery string
		if sortStart < sortEnd {
			midQuery = buildRangeQuery(query, sort, sortStart, sortMid)
		} else {
			midQuery = buildRangeQuery(query, sort, sortMid, sortStart)
		}
		midCount, err := client.count(index, midQuery)
		if err != nil {
			return 0, 0, err
		}
		newFrom = from - midCount
		if newFrom < 0 {
			newEnd = sortMid
		} else {
			newStart = sortMid
			if newFrom <= maxFrom {
				break
			}
		}
	}

	return newStart, newFrom, nil
}

// Add range restrictions to the original query.
func buildCmpQuery(query string, sort string, cmp string, value int64) string {
	template := "{\"bool\":{\"must\":%s,\"filter\":{\"range\":{\"%s\":{\"%s\":%d}}}}}"
	return fmt.Sprintf(template, query, sort, cmp, value)
}

// Add range restrictions to the original query.
func buildRangeQuery(query string, sort string, start int64, end int64) string {
	template := "{\"bool\":{\"must\":%s,\"filter\":{\"range\":{\"%s\":{\"gte\":%d,\"lte\":%d}}}}}"
	return fmt.Sprintf(template, query, sort, start, end)
}

// Call elasticsearch's countAPI to get the total number of documents that meet query conditions.
func (client *DeepPageClient) count(index string, query string) (int64, error) {
	url := index + "/_count"
	body := "{\"query\": " + query + "}"
	resp, err := client.postJson(url, body)
	if err != nil {
		return 0, err
	}

	var result map[string]any
	json.Unmarshal([]byte(resp), &result)

	count := int64(result["count"].(float64))
	return count, nil
}

// Call elasticsearch's searchAPI to get the documents that meet the conditions.
func (client *DeepPageClient) query(index string, query string, source *[]string, sort string, asc bool, from int64, size int64) (*[]map[string]any, error) {

	url := index + "/_search"

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("{")
	queryBuilder.WriteString("\"query\": " + query + ",")
	queryBuilder.WriteString("\"sort\": {\"" + sort + "\":\"" + iif(asc, "asc", "desc") + "\"},")
	if source != nil {
		sourceStr := make([]string, 0)
		for _, value := range *source {
			sourceStr = append(sourceStr, "\""+value+"\"")
		}
		queryBuilder.WriteString("\"_source\": " + "[" + strings.Join(sourceStr, ",") + "],")
	}
	queryBuilder.WriteString("\"from\":" + strconv.FormatInt(from, 10) + ",")
	queryBuilder.WriteString("\"size\":" + strconv.FormatInt(size, 10))
	queryBuilder.WriteString("}")

	resp, err := client.postJson(url, queryBuilder.String())
	if err != nil {
		return nil, err
	}

	var result map[string]any
	json.Unmarshal([]byte(resp), &result)

	var hits []map[string]any
	for _, value := range (result["hits"].(map[string]any))["hits"].([]any) {
		hits = append(hits, value.(map[string]any))
	}
	return &hits, nil
}

// Call elasticsearch low level rest client, post json to elasticsearch cluster.
func (client *DeepPageClient) postJson(url string, body string) (string, error) {

	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return "", err
	}

	resp, err := client.Transport.Perform(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	err = iif(resp.StatusCode == 200, nil, errors.New(resp.Status))
	return string(bytes), err
}
