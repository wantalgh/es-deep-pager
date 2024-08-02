/**
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

package wantalgh.esdeeppager;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Elasticsearch cluster deep paging query client.
 *
 * This client provides the search function of elasticsearch.
 * Using this client to query data from the elasticsearch cluster, you can use very large "from" and "size" parameter values
 * without changing the default max_result_window setting of the index.
 *
 * Reference:
 * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html">Paginate Search</a>
 */
public class DeepPageClient {

    private final RestClient restClient;

    private final long MAX_FROM = 2000L;

    private final long MAX_SIZE = 3000L;

    /**
     * Deep paging query client constructor
     * @param esRestClient elasticsearch low level rest client
     */
    public DeepPageClient(RestClient esRestClient) {
        this.restClient = esRestClient;
    }

    /**
     * Search method, receives parameters such as index, queryDsl, from, size, etc., and call the searchAPI 
     * of elasticsearch to query data.
     * Reference:
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html">Search API</a>
     * @param index
     * The index name for query, can use wildcards, e.g. my-index-*. This will be placed in search request path.
     * For lower versions of elasticsearch, it can contains type, e.g. my-index/my-type.
     * Reference:
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">Path parameters</a>
     * @param query
     * Query Dsl for query, this is a json formatted string, e.g. {"match_all":{}}.
     * This will be placed in the "query" field of the request body.
     * If not specified, the search will return all documents in the index.
     * Reference:
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html">Query Dsl</a>
     * @param source
     * source filter for query, e.g. ["column1", "column2", "obj1.*", "obj2.*" ]. 
     * This will be placed in the "_source" field of the request body.
     * If not specified, The query will return fields based on the default settings of the index.
     * Reference:
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html">Source Filtering</a>
     * @param sort
     * The sort field for query, e.g. "id". This will be placed in the "sort" field of the request body.
     * In order to implement fast large from parameter query, the queried data must be a well-ordered set. 
     * All the documents to be queried must have at least one unique number field, which is a numeric type and stores the 
     * unique number of each document. The available range of the number is the entire long integer, which can be negative 
     * and discontinuous, but the number of each document must not be repeated.
     * When performing fast from query, a unique number field must be used as sorting.
     * @param asc
     * Sort order of the unique number field, if true, means ascending, if false, means descending.  
     * @param from
     * Starting document offset, how many documents to skip. a non-negative number. e.g. 100000000
     * Using this client, you can use very large from parameter without changing the default max_result_window setting of the index.
     * @param size
     * The number of hits to return. a non-negative number. e.g. 1000000
     * Using this client, you can use large value parameter without changing the default max_result_window setting of the index.
     * @return
     * A list of all documents that match the query.
     * Each document is a json formatted string, you can choose your favorite json deserializer to parse it.
     * If no documents match the query, an empty list is returned.
     * @throws IOException
     * When an IO error occurs during the search process.
     */
    public List<String> search(String index, String query, Collection<String> source, String sort, boolean asc, long from, long size) throws IOException {

        // validate parameters
        if (index == null || index.isEmpty()) {
            throw new IllegalArgumentException("index can not be null or empty");
        }
        if (sort == null || sort.isEmpty()) {
            throw new IllegalArgumentException("sort can not be null or empty");
        }
        if (from < 0 || size < 0) {
            throw new IllegalArgumentException("from and size can not be negative");
        }
        if (size == 0) {
            return Collections.emptyList();
        }
        if (query == null) {
            query = "{\"match_all\":{}}";
        }

        // When the queried data is near the end of the data set, reverse the query direction.
        boolean reverse = false;
        if (from > MAX_FROM) {
            long total = count(index, query);
            if (total == 0 || from >= total) {
                return Collections.emptyList();
            }
            reverse = from > (total - from);
            if (reverse) {
                asc = !asc;
                long from2 = (total - from - size);
                long size2 = from2 < 0 ? size + from2 : size;
                from = Math.max(from2, 0);
                size = Math.max(size2, 0);
                if (size == 0) {
                    return Collections.emptyList();
                }
            }
        }

        // When the from parameter is large, find a sort value that can exclude some of the from data, and reduce the from value.
        String newQuery = query;
        long newFrom = from;
        if (from > MAX_FROM) {
            Map<String, Object> minItem = query(index, query, Collections.singleton(sort), sort, true, 0, 1).get(0);
            long sortMin = Long.parseLong((String)((Map)minItem.get("\"_source\"")).get("\"" + sort + "\""));
            Map<String, Object> maxItem = query(index, query, Collections.singleton(sort), sort, false, 0, 1).get(0);
            long sortMax = Long.parseLong((String)((Map)maxItem.get("\"_source\"")).get("\"" + sort + "\""));

            Map.Entry<Long, Long> startFrom;
            if (asc) {
                startFrom = findNewFrom(index, query, sort, sortMin, sortMax, from);
                newQuery = buildRangeQuery(query, sort, "gt", startFrom.getKey());
            } else {
                startFrom = findNewFrom(index, query, sort, sortMax, sortMin, from);
                newQuery = buildRangeQuery(query, sort, "lt", startFrom.getKey());
            }
            newFrom = startFrom.getValue();
        }

        // When the size parameter is large, query data in batches to reduce the size value.
        long remainSize = size;
        long retrieveSize = Math.min(size, MAX_SIZE);
        List<Map<String, Object>> batch = query(index, newQuery, source, sort, asc, newFrom, retrieveSize);
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> list = new ArrayList<>(batch);
        remainSize -= batch.size();
        while (remainSize > 0) {
            Map<String, Object> lastItem = batch.get(batch.size() - 1);
            long lastSort = Long.parseLong((String)((List)lastItem.get("\"sort\"")).get(0));
            if (asc) {
                newQuery = buildRangeQuery(query, sort, "gt", lastSort);
            } else {
                newQuery = buildRangeQuery(query, sort, "lt", lastSort);
            }
            retrieveSize = Math.min(remainSize, MAX_SIZE);
            batch = query(index, newQuery, source, sort, asc, 0, retrieveSize);
            if (batch.isEmpty()) {
                break;
            }
            list.addAll(batch);
            remainSize -= batch.size();
        }

        // If result is reverse query data, reverse it back.
        if (reverse) {
            Collections.reverse(list);
        }

        return list.stream().map(EsJsonAnalyzer::toJson).collect(Collectors.toList());
    }

    /**
     * Add range restrictions to the original query.
     */
    private String buildRangeQuery(String query, String sort, long start, long end) {
        String template = "{\"bool\":{\"must\":%s,\"filter\":{\"range\":{\"%s\":{\"lte\":%d,\"gte\":%d}}}}}";
        return String.format(template, query, sort, end, start);
    }

    /**
     * Add range restrictions to the original query.
     */
    private String buildRangeQuery(String query, String sort, String cmp, long value) {
        String template = "{\"bool\":{\"must\":%s,\"filter\":{\"range\":{\"%s\":{\"%s\":%d}}}}}";
        return String.format(template, query, sort, cmp, value);
    }

    /**
     * Use binary search to find new query parameters with the same result as the original query but with a smaller from value
     */
    private Map.Entry<Long, Long> findNewFrom(String index, String query, String sort, long sortStart, long sortEnd, long from) throws IOException {
        long newStart = sortStart;
        long newEnd = sortEnd;
        long newFrom;
        while (true) {
            long sortMin = Math.min(newStart, newEnd);
            long sortAbs = Math.abs(newStart - newEnd);
            if (sortAbs <= 1) {
                // sort duplicated or data changed
                return new AbstractMap.SimpleEntry<>(sortMin, sortAbs);
            }
            long sortMid = Double.valueOf(sortMin + sortAbs * 0.5).longValue();
            String midQuery;
            if (sortStart < sortEnd) {
                midQuery = buildRangeQuery(query, sort, sortStart, sortMid);
            } else {
                midQuery = buildRangeQuery(query, sort, sortMid, sortStart);
            }
            long midCount = count(index, midQuery);
            newFrom = from - midCount;
            if (newFrom < 0) {
                newEnd = sortMid;
            } else {
                newStart = sortMid;
                if(newFrom <= MAX_FROM) {
                    break;
                }
            }
        }
        return new AbstractMap.SimpleEntry<>(newStart, newFrom);
    }

    /**
     * Call elasticsearch's searchAPI to get the documents that meet the conditions.
     */
    private List<Map<String, Object>> query(String index, String query, Collection<String> source, String sort, boolean asc, long from, long size) throws IOException {
        String url = index + "/_search";

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("{");
        queryBuilder.append("\"query\":" + query + ",");
        queryBuilder.append("\"sort\":{\"" + sort + "\":\"" + (asc ? "asc" : "desc") + "\"},");
        if (source != null) {
            String sourceStr = source.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(","));
            queryBuilder.append("\"_source\":[" + sourceStr + "],");
        }
        queryBuilder.append("\"from\":" + from + ",");
        queryBuilder.append("\"size\":" + size);
        queryBuilder.append("}");

        String json = queryBuilder.toString();
        String response = postJson(url, json);
        Map<String, Object> map = (Map) EsJsonAnalyzer.toObject(response);
        return  (List)((Map) map.get("\"hits\"")).get("\"hits\"");
    }

    /**
     * Call elasticsearch's countAPI to get the total number of documents that meet query conditions.
     */
    private long count(String index, String query) throws IOException {
        String url = index + "/_count";
        String json = "{\"query\": " +  query + "}";
        String response = postJson(url, json);
        Map<String, Object> map = (Map) EsJsonAnalyzer.toObject(response);
        return Long.parseLong((String)map.get("\"count\""));
    }

    /**
     * Call elasticsearch low level rest client, post json to elasticsearch cluster.
     */
    private String postJson(String url, String json) throws IOException {
        Request request = new Request("POST", url);
        request.setJsonEntity(json);
        Response response = restClient.performRequest(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Encountered error response code: " + response.getStatusLine().getStatusCode());
        }
        return EntityUtils.toString(response.getEntity());
    }

    /**
     * Simple json analyzer.
     * In order to keep dependencies low, use this own json analyzer.
     */
    private static class EsJsonAnalyzer {

        private final String json;

        private final int length;

        private int position;

        private Character currentChar;

        // Constructor
        private EsJsonAnalyzer(String json) {
            this.json = json.trim();
            this.length = json.length();
            this.position = 0;
            this.currentChar = this.json.charAt(this.position);
        }

        // Goto next json character.
        private boolean gotoNextChar() {
            position++;
            if (position < length) {
                currentChar = json.charAt(position);
                return true;
            } else {
                return false;
            }
        }

        // Count the number of consecutive occurrences of a specified character before the current character
        private int countPrevChar(char checkChar) {
            int charCount = 0;
            int prevPosition = position - 1;
            while (prevPosition >= 0) {
                char prevChar = json.charAt(prevPosition);
                if (checkChar == prevChar) {
                    charCount++;
                } else {
                    break;
                }
                prevPosition--;
            }
            return charCount;
        }

        // Read a json array.
        private List<Object> readJsonArray() {
            assert currentChar == '[';
            gotoNextChar();
            List<Object> list = new ArrayList<>();
            int lastPosition = 0;
            while (true) {
                if (lastPosition == position) {
                    throw new RuntimeException("invalid json format");
                }
                lastPosition = position;

                skipSpace();
                if (currentChar == ',') {
                    gotoNextChar();
                    continue;
                }
                if (currentChar == ']') {
                    gotoNextChar();
                    break;
                }
                list.add(readJsonValue());
            }
            return list;
        }

        // Read a json object.
        private Map<String, Object> readJsonObject() {
            assert currentChar == '{';
            gotoNextChar();
            Map<String, Object> map = new TreeMap<>();
            int lastPosition = 0;
            while (true) {
                if (lastPosition == position) {
                    throw new RuntimeException("invalid json format");
                }
                lastPosition = position;

                skipSpace();
                if (currentChar == ',') {
                    gotoNextChar();
                    continue;
                }
                if (currentChar == '}') {
                    gotoNextChar();
                    break;
                }
                Map.Entry<String, Object> item = readJsonKeyValue();
                map.put(item.getKey(), item.getValue());
            }
            return map;
        }

        private static final Character[] stopChars = new Character[]{' ', '\t', '\r', '\n', ',', ']', '}'};

        // Read a json literal.
        private String readJsonLiteral() {
            StringBuilder stringBuilder = new StringBuilder();
            do {
                if (Arrays.stream(stopChars).anyMatch(c -> c.equals(currentChar))) {
                    break;
                }
                stringBuilder.append(currentChar);
            } while (gotoNextChar());
            return stringBuilder.toString();
        }

        // Read a json string.
        private String readJsonString() {
            assert currentChar == '\"';
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(currentChar);
            while (gotoNextChar()) {
                stringBuilder.append(currentChar);
                if (currentChar == '\"' && countPrevChar('\\') % 2 == 0) {
                    gotoNextChar();
                    break;
                }
            } ;
            return stringBuilder.toString();
        }

        // Read a json key-value pair.
        private Map.Entry<String, Object> readJsonKeyValue() {
            skipSpace();
            assert currentChar == '\"';
            String key = readJsonString();
            skipSpace();
            assert currentChar == ':';
            gotoNextChar();
            skipSpace();
            Object value = readJsonValue();
            return new AbstractMap.SimpleEntry<>(key, value);
        }

        // Read a json value.
        private Object readJsonValue() {
            switch (currentChar) {
                case '\"':
                    return readJsonString();
                case '{':
                    return readJsonObject();
                case '[':
                    return readJsonArray();
                default:
                    return readJsonLiteral();
            }
        }

        private static final Character[] spaceChars = new Character[]{' ', '\t', '\n', '\r'};
        // Skip space characters
        private int skipSpace() {
            int count = 0;
            while (Arrays.stream(spaceChars).anyMatch(c -> c.equals(currentChar))) {
                if (!gotoNextChar()) {
                    break;
                }
                count++;
            }
            return count;
        }

        /**
         * Deserialize json to object.
         */
        public static Object toObject(String json) {
            EsJsonAnalyzer analyzer = new EsJsonAnalyzer(json);
            return analyzer.readJsonValue();
        }

        /**
         * Serialize object to json.
         */
        public static String toJson(Object value) {
            if (value instanceof Map) {
                Map<String, Object> entries = ((Map) value);
                String properties = entries.entrySet().stream()
                        .map(e -> e.getKey() + ":" + toJson(e.getValue()))
                        .collect(Collectors.joining(","));
                return "{" + properties + "}";
            } else if (value instanceof List) {
                List<Object> values = ((List) value);
                String items = values.stream()
                        .map(EsJsonAnalyzer::toJson)
                        .collect(Collectors.joining(","));
                return "[" + items + "]";
            } else {
                return String.valueOf(value);
            }
        }
    }
}


