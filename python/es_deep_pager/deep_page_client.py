
# MIT License

# Copyright (c) 2024 wantalgh

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from typing import List
from io import StringIO
from elastic_transport import Transport

class DeepPageClient:
    """
    Elasticsearch cluster deep paging query client.
    This client provides the search function of elasticsearch.
    Using this client to query data from the elasticsearch cluster, you can use very large "from" and "size" parameter values
    without changing the default max_result_window setting of the index.
    Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
    """

    __transport = None
    __max_from = 2000
    __max_size = 3000


    def __init__(self, transport: Transport) -> None:
        """
        Deep paging query client initialization.
        """
        self.__transport = transport


    def search(self, index: str, query: str, source: List[str], sort: str, asc: bool, s_from: int, s_size: int) -> List[dict]:
        """
        Search method, receives parameters such as index, queryDsl, from, size, etc., and call the searchAPI of elasticsearch to query data.
        Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html

        Parameters
        ----------
        index: str
            The index name for query, can use wildcards, e.g. my-index-*. This will be placed in search request path.
            For lower versions of elasticsearch, it can contains type, e.g. my-index/my-type.
            Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
        query: str
            Query Dsl for query, this is a json formatted string, e.g. {"match_all":{}}.
            This will be placed in the "query" field of the request body.
            If not specified, the search will return all documents in the index.
            Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
        source: List[str]
            source filter for query, e.g. ["column1", "column2", "obj1.*", "obj2.*" ].
            This will be placed in the "_source" field of the request body.
            If not specified, The query will return fields based on the default settings of the index.
            Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html
        sort: str
            The sort field for query, e.g. "id". This will be placed in the "sort" field of the request body.
            In order to implement fast large from parameter query, the queried data must be a well-ordered set.
            All the documents to be queried must have at least one unique number field, which is a numeric type and stores the 
            unique number of each document. The available range of the number is the entire long integer, which can be negative
            and discontinuous, but the number of each document must not be repeated.
            When performing fast from query, a unique number field must be used as sorting.
        asc: bool
            Sort order of the unique number field, if true, means ascending, if false, means descending. 
        s_from: int
            Starting document offset, how many documents to skip. a non-negative number. e.g. 100000000 
            Using this client, you can use very large from parameter without changing the default max_result_window setting of the index.
        s_size: int
            The number of hits to return. a non-negative number. e.g. 1000000 
            Using this client, you can use large value parameter without changing the default max_result_window setting of the index.

        Returns
        -------
        List[dict]
            A list of all documents that match the query. Each document has been converted into a Python built-in dictionary.
            If no documents match the query, an empty list is returned.
        """

        # validate parameters
        if (index is None or index == ""):
            raise Exception("index is required")

        if (sort is None or sort == ""):
            raise Exception("sort is required")

        if (s_from is None or s_from < 0):
            raise Exception("s_from can not be negative")
        
        if (s_size is None or s_size < 0):
            raise Exception("s_size can not be negative")

        if (s_size == 0):
            return []
        
        if (query is None):
            query = "{\"match_all\":{}}"

        # When the queried data is near the end of the data set, reverse the query direction.
        reverse = False
        if (s_from > self.__max_from):
            total = self.__count(index, query)
            if total == 0 or s_from >= total:
                return []
            reverse = s_from > (total - s_from)
            if reverse:
                asc = not asc
                from2 = (total - s_from - s_size)
                size2 = s_size + from2 if from2 < 0 else s_size
                s_from = max(from2, 0)
                s_size = max(size2, 0)
                if s_size == 0:
                    return []
                pass
            pass
        pass

        # When the from parameter is large, find a sort value that can exclude some of the from data, and reduce the from value.
        new_query = query
        new_from = s_from
        if (s_from > self.__max_from):
            min_item = self.__query(index, query, [sort], sort, True, 0, 1)[0]
            sort_min = min_item["_source"][sort]
            max_item = self.__query(index, query, [sort], sort, False, 0, 1)[0]
            sort_max = max_item["_source"][sort]

            if (asc):
                (new_start, new_from) = self.__find_new_from(index, query, sort, sort_min, sort_max, s_from)
                new_query = self.__build_cmp_query(query, sort, "gt", new_start)
            else:
                (new_start, new_from) = self.__find_new_from(index, query, sort, sort_max, sort_min, s_from)
                new_query = self.__build_cmp_query(query, sort, "lt", new_start)
            pass

        # When the size parameter is large, query data in batches to reduce the size value.
        remain_size = s_size
        retrieve_size = min(s_size, self.__max_size)
        batch = self.__query(index, new_query, source, sort, asc, new_from, retrieve_size)
        if len(batch) == 0:
            return []
        result = list(batch)
        remain_size -= len(batch)

        while remain_size > 0:
            last_sort = result[-1]["_source"][sort]
            if asc:
                new_query = self.__build_cmp_query(query, sort, "gt", last_sort)
            else:
                new_query = self.__build_cmp_query(query, sort, "lt", last_sort)

            retrieve_size = min(remain_size, self.__max_size)
            batch = self.__query(index, new_query, source, sort, asc, 0, retrieve_size)
            if len(batch) == 0:
                break
            result.extend(batch)
            remain_size -= len(batch)
        
        if reverse:
            result.reverse()
            
        return result
            

    @staticmethod
    def __build_cmp_query(query: str, sort: str, cmp: str, value: int) -> str:
        """
        Add range restrictions to the original query.
        """
        return f"{{\"bool\":{{\"must\":{query},\"filter\":{{\"range\":{{\"{sort}\":{{\"{cmp}\":{value}}}}}}}}}}}"


    @staticmethod
    def __build_range_query(query: str, sort: str, start: int, end: int) -> str:
        """
        Add range restrictions to the original query.
        """
        return f"{{\"bool\":{{\"must\":{query},\"filter\":{{\"range\":{{\"{sort}\":{{\"gte\":{start},\"lte\":{end}}}}}}}}}}}"


    def __find_new_from(self, index: str, query: str, sort: str, sort_start: int, sort_end: int, s_from: int) -> tuple:
        """
        Use binary search to find new query parameters with the same result as the original query but with a smaller from value.
        """
        new_start = sort_start
        new_end = sort_end
        while True:
            sort_min = min(new_start, new_end)
            sort_abs = abs(new_start - new_end)

            if (sort_abs <= 1):
                return (sort_min, sort_abs)
            
            sort_mid = sort_min + sort_abs // 2
            if sort_start < sort_end:
                mid_query = self.__build_range_query(query, sort, sort_start, sort_mid)
            else:
                mid_query = self.__build_range_query(query, sort, sort_mid, sort_start)
            mid_count = self.__count(index, mid_query)
            new_from = s_from - mid_count

            if new_from < 0:
                new_end = sort_mid
            else:
                new_start = sort_mid
                if new_from <= self.__max_from:
                    break
                pass
            pass
        return (new_start, new_from)
        

    def __query(self, index: str, query: str, source: List[str], sort: str, asc: bool, s_from: int, s_size: int) -> list:
        """
        Call elasticsearch's searchAPI to get the documents that meet the conditions.
        """
        url = f"/{index}/_search"
        data_io = StringIO()
        data_io.write("{")
        data_io.write(f"\"query\": {query},")
        data_io.write(f"\"sort\": {{\"{sort}\": \"{'asc' if asc else 'desc'}\"}},")
        if source is not None:
            source_str = ",".join([ f"\"{s}\"" for s in source ])
            data_io.write(f"\"_source\": [{source_str}],")
        data_io.write(f"\"from\": {s_from},")
        data_io.write(f"\"size\": {s_size}")
        data_io.write("}")
        data = data_io.getvalue()
        response = self.__post_json(url, data)
        return response["hits"]["hits"]


    def __count(self, index: str, query: str) -> int:
        """
        Call elasticsearch's countAPI to get the total number of documents that meet query conditions.
        """
        url = f"/{index}/_count"
        body = "{\"query\": " + query + "}"
        response = self.__post_json(url, body)
        return response["count"]


    def __post_json(self, url: str, body: str) -> str:
        """
        Call elasticsearch transport client, post json to elasticsearch cluster.
        """
        response = self.__transport.perform_request("POST", url, headers={"Content-type": "application/json"}, body=body)
        return response.body