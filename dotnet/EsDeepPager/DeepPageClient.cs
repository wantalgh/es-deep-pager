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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using Elastic.Transport;

namespace Wantalgh.EsDeepPager
{
    /// <summary>
    /// Elasticsearch cluster deep paging query client.
    ///
    /// This client provides the search function of elasticsearch.
    /// Using this client to query data from the elasticsearch cluster, you can use very large "from" and "size" parameter values
    /// without changing the default max_result_window setting of the index.
    ///
    /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
    /// </summary>
    public class DeepPageClient
    {
        private readonly ITransport _transport;

        private const long MaxFrom = 2000;
        private const long MaxSize = 3000;

        /// <summary>
        /// Deep paging query client constructor
        /// </summary>
        /// <param name="transport">
        /// elasticsearch low level rest client.
        /// Reference: https://github.com/elastic/elastic-transport-net
        /// </param>
        public DeepPageClient(ITransport transport)
        {
            _transport = transport;
        }

        /// <summary>
        /// Search method, receives parameters such as index, queryDsl, from, size, etc., and call the searchAPI
        /// of elasticsearch to query data.
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html
        /// </summary>
        /// <param name="index">
        /// The index name for query, can use wildcards, e.g. my-index-*. This will be placed in search request path.
        /// For lower versions of elasticsearch, it can contains type, e.g. my-index/my-type.
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
        /// </param>
        /// <param name="query">
        /// Query Dsl for query, this is a json formatted string, e.g. {"match_all":{}}.
        /// This will be placed in the "query" field of the request body.
        /// If not specified, the search will return all documents in the index.
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
        /// </param>
        /// <param name="source">
        /// source filter for query, e.g. ["column1", "column2", "obj1.*", "obj2.*" ].
        /// This will be placed in the "_source" field of the request body.
        /// If not specified, The query will return fields based on the default settings of the index.
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html
        /// </param>
        /// <param name="sort">
        /// The sort field for query, e.g. "id". This will be placed in the "sort" field of the request body.
        /// In order to implement fast large from parameter query, the queried data must be a well-ordered set.
        /// All the documents to be queried must have at least one unique number field, which is a numeric type and stores the 
        /// unique number of each document. The available range of the number is the entire long integer, which can be negative
        /// and discontinuous, but the number of each document must not be repeated.
        /// When performing fast from query, a unique number field must be used as sorting.
        /// </param>
        /// <param name="asc">
        /// Sort order of the unique number field, if true, means ascending, if false, means descending.  
        /// </param>
        /// <param name="from">
        /// Starting document offset, how many documents to skip. a non-negative number. e.g. 100000000
        /// Using this client, you can use very large from parameter without changing the default max_result_window setting of the index.
        /// </param>
        /// <param name="size">
        /// The number of hits to return. a non-negative number. e.g. 1000000
        /// Using this client, you can use large value parameter without changing the default max_result_window setting of the index.
        /// </param>
        /// <returns>
        /// A list of all documents that match the query.
        /// Each document is a json formatted string, you can choose your favorite json deserializer to parse it.
        /// If no documents match the query, an empty list is returned.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="index"/> or <paramref name="sort"/> is null
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="from"/> or <paramref name="size"/> is less than 0
        /// </exception>
        public IEnumerable<string> Search(string index, string query, IEnumerable<string> source, string sort, bool asc,
            long from, long size)
        {
            if (string.IsNullOrEmpty(index))
            {
                throw new ArgumentNullException(nameof(index));
            }

            if (string.IsNullOrEmpty(sort))
            {
                throw new ArgumentNullException(nameof(sort));
            }

            if (from < 0 || size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(from) + " or " + nameof(size));
            }

            if (size == 0)
            {
                return new string[] { };
            }

            if (query == null)
            {
                query = "{\"match_all\":{}}";
            }

            source = source?.ToArray();

            // When the queried data is near the end of the data set, reverse the query direction.
            var reverse = false;
            if (from > MaxFrom)
            {
                var total = Count(index, query);
                if (total == 0 || from >= total)
                {
                    return new string[] { };
                }

                reverse = from > (total - from);
                if (reverse)
                {
                    asc = !asc;
                    var from2 = total - from - size;
                    var size2 = from2 < 0 ? size + from2 : size;
                    from = Math.Max(from2, 0);
                    size = Math.Max(size2, 0);
                    if (size == 0)
                    {
                        return new string[] { };
                    }
                }
            }

            // When the from parameter is large, find a sort value that can exclude some of the from data, and reduce the from value.
            var newQuery = query;
            var newFrom = from;
            if (from > MaxFrom)
            {
                var minItem = Query(index, query, new[] { sort }, sort, true, 0, 1)[0];
                var sortMin = minItem.GetProperty("_source").GetProperty(sort).GetInt64();
                var maxItem = Query(index, query, new[] { sort }, sort, false, 0, 1)[0];
                var sortMax = maxItem.GetProperty("_source").GetProperty(sort).GetInt64();

                long newStart;
                if (asc)
                {
                    (newStart, newFrom) = FindNewFrom(index, query, sort, sortMin, sortMax, from);
                    newQuery = BuildRangeQuery(query, sort, "gt", newStart);
                }
                else
                {
                    (newStart, newFrom) = FindNewFrom(index, query, sort, sortMax, sortMin, from);
                    newQuery = BuildRangeQuery(query, sort, "lt", newStart);
                }
            }

            // When the size parameter is large, query data in batches to reduce the size value.
            var remainSize = size;
            var retrieveSize = Math.Min(size, MaxSize);
            var batch = Query(index, newQuery, source, sort, asc, newFrom, retrieveSize);
            if (batch.Length == 0)
            {
                return new string[] { };
            }

            var list = new List<JsonElement>(batch);
            remainSize -= batch.Length;
            while (remainSize > 0)
            {
                var lastSort = batch.Last().GetProperty("sort")[0].GetInt64();
                if (asc)
                {
                    newQuery = BuildRangeQuery(query, sort, "gt", lastSort);
                }
                else
                {
                    newQuery = BuildRangeQuery(query, sort, "lt", lastSort);
                }

                retrieveSize = Math.Min(remainSize, MaxSize);
                batch = Query(index, newQuery, source, sort, asc, 0, retrieveSize);
                if (batch.Length == 0)
                {
                    break;
                }

                list.AddRange(batch);
                remainSize -= batch.Length;
            }

            // If result is reverse query data, reverse it back.
            if (reverse)
            {
                list.Reverse(); 
            }

            return list.Select(i => i.ToString());
        }

        /// <summary>
        /// Use binary search to find new query parameters with the same result as the original query but with a smaller from value
        /// </summary>
        private (long start, long from) FindNewFrom(string index, string query, string sort, long sortStart,
            long sortEnd, long from)
        {
            var newStart = sortStart;
            var newEnd = sortEnd;
            long newFrom;
            while (true)
            {
                var sortMin = Math.Min(newStart, newEnd);
                var sortAbs = Math.Abs(newStart - newEnd);
                if (sortAbs <= 1)
                {
                    // sort duplicated or data changed
                    return (sortMin, sortAbs);
                }

                var sortMid = sortMin + sortAbs / 2;
                string midQuery;
                if (sortStart < sortEnd)
                {
                    midQuery = BuildRangeQuery(query, sort, sortStart, sortMid);
                }
                else
                {
                    midQuery = BuildRangeQuery(query, sort, sortMid, sortStart);
                }

                var midCount = Count(index, midQuery);
                newFrom = from - midCount;
                if (newFrom < 0)
                {
                    newEnd = sortMid;
                }
                else
                {
                    newStart = sortMid;
                    if (newFrom <= MaxFrom)
                    {
                        break;
                    }
                }

            }

            return (newStart, newFrom);
        }

        /// <summary>
        /// Call elasticsearch's searchAPI to get the documents that meet the conditions.
        /// </summary>
        private JsonElement[] Query(string index, string query, IEnumerable<string> source, string sort, bool asc,
            long from, long size)
        {
            var url = index + "/_search";

            var queryBuilder = new StringBuilder();
            queryBuilder.Append("{");
            queryBuilder.Append("\"query\": " + query + ",");
            queryBuilder.Append("\"sort\":{\"" + sort + "\":\"" + (asc ? "asc" : "desc") + "\"},");
            if (source != null)
            {
                queryBuilder.Append("\"_source\":[" + string.Join(",", source.Select(i => "\"" + i + "\"")) + "],");
            }

            queryBuilder.Append("\"from\":" + from + ",");
            queryBuilder.Append("\"size\":" + size);
            queryBuilder.Append("}");
            var json = queryBuilder.ToString();

            var response = PostJson(url, json);
            var doc = JsonDocument.Parse(response);
            var element = doc.RootElement.GetProperty("hits").GetProperty("hits");
            return element.EnumerateArray().ToArray();
        }

        /// <summary>
        /// Call elasticsearch low level rest client, post json to elasticsearch cluster.
        /// </summary>
        private string PostJson(string url, string json)
        {
            var response = _transport.Request<StringResponse>(HttpMethod.POST, url, json);
            return response.Body;
        }

        /// <summary>
        /// Call elasticsearch's countAPI to get the total number of documents that meet query conditions.
        /// </summary>
        private long Count(string index, string query)
        {
            var url = index + "/_count";
            var json = "{\"query\": " + query + "}";
            var response = PostJson(url, json);
            var doc = JsonDocument.Parse(response);
            return doc.RootElement.GetProperty("count").GetInt64();
        }

        /// <summary>
        /// Add range restrictions to the original query.
        /// </summary>
        private string BuildRangeQuery(string query, string sort, string cmp, long value)
        {
            return $"{{\"bool\":{{\"must\":{query},\"filter\":{{\"range\":{{\"{sort}\":{{\"{cmp}\":{value}}}}}}}}}}}";
        }

        /// <summary>
        /// Add range restrictions to the original query.
        /// </summary>
        private string BuildRangeQuery(string query, string sort, long start, long end)
        {
            return
                $"{{\"bool\":{{\"must\":{query},\"filter\":{{\"range\":{{\"{sort}\":{{\"gte\":{start},\"lte\":{end}}}}}}}}}}}";
        }
    }
}
