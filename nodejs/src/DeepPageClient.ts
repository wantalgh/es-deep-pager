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

import { Transport } from '@elastic/transport'

/**
 * Elasticsearch cluster deep paging query client.
 *
 * This client provides the search function of elasticsearch.
 * Using this client to query data from the elasticsearch cluster, you can use very large "from" and "size" parameter values
 * without changing the default max_result_window setting of the index.
 *
 * Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
 */
export default class DeepPageClient {
  private readonly transport: Transport

  private static readonly MAX_FROM = 2000
  private static readonly MAX_SIZE = 3000

  /**
     * Deep paging query client constructor
     * @param transport elasticsearch low level rest client.
     * Reference: https://github.com/elastic/elastic-transport-js
     */
  constructor (transport: Transport) {
    this.transport = transport
  }

  /**
     * Search method, receives parameters such as index, queryDsl, from, size, etc., and call the searchAPI of elasticsearch to query data.
     * Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html
     *
     * @param index
     * The index name for query, can use wildcards, e.g. my-index-*. This will be placed in search request path.
     * For lower versions of elasticsearch, it can contains type, e.g. my-index/my-type.
     * Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
     *
     * @param query
     * Query Dsl for query, this is a json formatted string, e.g. {"match_all":{}}.
     * This will be placed in the "query" field of the request body.
     * If not specified, the search will return all documents in the index.
     * Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
     *
     * @param source
     * source filter for query, e.g. ["column1", "column2", "obj1.*", "obj2.*" ].
     * This will be placed in the "_source" field of the request body.
     * If not specified, The query will return fields based on the default settings of the index.
     * Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html
     *
     * @param sort
     * The sort field for query, e.g. "id". This will be placed in the "sort" field of the request body.
     * In order to implement fast large from parameter query, the queried data must be a well-ordered set.
     * All the documents to be queried must have at least one unique number field, which is a numeric type and stores the
     * unique number of each document. The available range of the number is the entire long integer, which can be negative
     * and discontinuous, but the number of each document must not be repeated.
     * When performing fast from query, a unique number field must be used as sorting.
     *
     * @param asc
     * Sort order of the unique number field, if true, means ascending, if false, means descending.
     *
     * @param from
     * Starting document offset, how many documents to skip. a non-negative number. e.g. 100000000
     * Using this client, you can use very large from parameter without changing the default max_result_window setting of the index.
     *
     * @param size
     * The number of hits to return. a non-negative number. e.g. 1000000
     * Using this client, you can use large value parameter without changing the default max_result_window setting of the index.
     *
     * @return
     * A list of all documents that match the query.
     * If no documents match the query, an empty list is returned.
     */
  public async search (index: string, query: string, source: string[], sort: string, asc: boolean, from: number, size: number) {
    // validate parameters
    if (index == null) {
      throw new Error('index cannot be null')
    }
    if (sort == null) {
      throw new Error('sort cannot be null')
    }
    if (from < 0 || size < 0) {
      throw new Error('from and size cannot be negative')
    }

    if (size == 0) {
      return []
    }
    if (query == null) {
      query = '{"match_all":{}}'
    }

    // When the queried data is near the end of the data set, reverse the query direction.
    let reverse = false
    if (from > DeepPageClient.MAX_FROM) {
      const total = await this.count(index, query)
      if (total == 0 || from > total) {
        return []
      }

      reverse = from > (total - from)
      if (reverse) {
        asc = !asc
        const from2 = total - from - size
        const size2 = from2 < 0 ? size + from2 : size
        from = Math.max(from2, 0)
        size = Math.max(size2, 0)

        if (size == 0) {
          return []
        }
      }
    }

    // When the from parameter is large, find a sort value that can exclude some of the from data, and reduce the from value.
    let newQuery = query
    let newFrom = from
    if (from > DeepPageClient.MAX_FROM) {
      const minItems = await this.query(index, query, [sort], sort, true, 0, 1)
      if (minItems.length == 0) {
        return []
      }
      const sortMin = minItems[0]._source[sort]
      const maxItems = await this.query(index, query, [sort], sort, false, 0, 1)
      if (maxItems.length == 0) {
        return []
      }
      const sortMax = maxItems[0]._source[sort]

      let newStart: number
      if (asc) {
        const startFrom = await this.findNewFrom(index, query, sort, sortMin, sortMax, from)
        newStart = startFrom.Start
        newFrom = startFrom.From
        newQuery = this.buildCmpQuery(query, sort, 'gt', newStart)
      } else {
        const startFrom = await this.findNewFrom(index, query, sort, sortMax, sortMin, from)
        newStart = startFrom.Start
        newFrom = startFrom.From
        newQuery = this.buildCmpQuery(query, sort, 'lt', newStart)
      }
    }

    // When the size parameter is large, query data in batches to reduce the size value.
    let remainSize = size
    let retrieveSize = Math.min(size, DeepPageClient.MAX_SIZE)
    let batch = await this.query(index, newQuery, source, sort, asc, newFrom, retrieveSize)
    if (batch.length == 0) {
      return []
    }
    let list = [...batch]
    remainSize -= batch.length
    while (remainSize > 0) {
      const lastSort = batch[batch.length - 1].sort[0]
      if (asc) {
        newQuery = this.buildCmpQuery(query, sort, 'gt', lastSort)
      } else {
        newQuery = this.buildCmpQuery(query, sort, 'lt', lastSort)
      }

      retrieveSize = Math.min(remainSize, DeepPageClient.MAX_SIZE)
      batch = await this.query(index, newQuery, source, sort, asc, 0, retrieveSize)
      if (batch.length == 0) {
        break
      }
      list = [...list, ...batch]
      remainSize -= batch.length
    }

    // If result is reverse query data, reverse it back.
    if (reverse) {
      list = list.reverse()
    }
    return list
  }

  /**
     * Call elasticsearch's searchAPI to get the documents that meet the conditions.
     */
  private async query (index: string, query: string, source: string[], sort: string, asc: boolean, from: number, size: number) {
    const url = `${index}/_search`
    const queryBuilder = []
    queryBuilder.push('{"query": ' + query + ',')
    queryBuilder.push('"sort":{"' + sort + '":"' + (asc ? 'asc' : 'desc') + '"},')
    if (source != null) {
      const source_str = source.map(x => '"' + x + '"').join(',')
      queryBuilder.push('"_source":[' + source_str + '],')
    }
    queryBuilder.push('"from":' + from + ',')
    queryBuilder.push('"size":' + size + '}')

    const body = queryBuilder.join('')
    const resp = await this.postJson(url, body) as Record<string, any>
    return resp.hits.hits
  }

  /**
     * Use binary search to find new query parameters with the same result as the original query but with a smaller from value.
     */
  private async findNewFrom (index: string, query: string, sort: string, sortStart: number, sortEnd: number, from: number) {
    let newStart = sortStart
    let newEnd = sortEnd
    let newFrom: number
    while (true) {
      const sortMin = Math.min(newStart, newEnd)
      const sortAbs = Math.abs(newStart - newEnd)
      if (sortAbs <= 1) {
        // sort duplicated or data changed
        return { Start: sortMin, From: sortAbs }
      }
      const sortMid = sortMin + Math.floor(sortAbs / 2)
      let midQuery: string
      if (sortStart < sortEnd) {
        midQuery = this.buildRangeQuery(query, sort, sortStart, sortMid)
      } else {
        midQuery = this.buildRangeQuery(query, sort, sortMid, sortStart)
      }

      const midCount = await this.count(index, midQuery)
      newFrom = from - midCount
      if (newFrom < 0) {
        newEnd = sortMid
      } else {
        newStart = sortMid
        if (newFrom <= DeepPageClient.MAX_FROM) {
          break
        }
      }
    }
    return { Start: newStart, From: newFrom }
  }

  /**
     * Add range restrictions to the original query.
     */
  private buildCmpQuery (query: string, sort: string, cmp: string, value: number) {
    return `{\"bool\":{\"must\":${query},\"filter\":{\"range\":{\"${sort}\":{\"${cmp}\":${value}}}}}}`
  }

  /**
     * Add range restrictions to the original query.
     */
  private buildRangeQuery (query: string, sort: string, start: number, end: number) {
    return `{\"bool\":{\"must\":${query},\"filter\":{\"range\":{\"${sort}\":{\"gte\":${start},\"lte\":${end}}}}}}`
  }

  /**
     * Call elasticsearch's countAPI to get the total number of documents that meet query conditions.
     */
  private async count (index: string, query: any) {
    const url = `${index}/_count`
    const body = `{\"query\":${query}}`
    const resp = await this.postJson(url, body) as Record<string, any>

    return resp.count
  }

  /**
     * Call elasticsearch low level rest client, post json to elasticsearch cluster.
     */
  private async postJson (path: string, body: string) {
    return await this.transport.request({ method: 'POST', path, body: JSON.parse(body) })
  }
}
