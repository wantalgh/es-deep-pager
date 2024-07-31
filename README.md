EsDeepPager. A elasticsearch client that can perform fast deep paging queries.
***

This project is a client for querying deep paging data in the elasticsearch cluster. The client provides a search method that accepts parameters such as index, queryDSL, source, from, size, etc., and uses these parameters to call elasticsearch's [SearchAPI](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html) to get data. Compared to calling the SearchAPI directly, this client search method can use very large from and size parameters to quickly query pre-numbered data without placing too much load on the elasticsearch cluster, and does not require modifying the index's max_result_window setting.  

This client is widely applicable and easy to use. It is designed to have only one class or file, and only relies on the minimum client officially provided by elasticsearch, so it can be easily introduced into other projects for use without causing negative impacts such as class library conflicts on the project.  

**算法介绍：**
**使用说明：**