
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

 //! Elasticsearch cluster deep paging query client for rust.
 //! 
 //! This mod provides the search client of elasticsearch. 
 //! Using this client to query data from the elasticsearch cluster, you can use very large "from" and "size" parameter values 
 //! without changing the default max_result_window setting of the index. 
 //! 
 //! Reference:
 //! https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html
pub mod deep_page_client{

    use elasticsearch::http::{headers::HeaderMap, transport::Transport};

    /// Error message.
    pub enum Error { Message(String) }

    /// Deep paging query client.
    /// 
    /// # Parameters
    ///
    /// * `transport`: 
    /// Elasticsearch official http transport. 
    /// Reference: https://github.com/elastic/elasticsearch-rs
    /// 
    /// # Examples
    /// 
    /// ```
    /// let client = deep_page_client::Client(transport);
    /// ```
    pub struct Client(pub Transport);

    const MAX_FROM : i64 = 2000;
    const MAX_SIZE : i64 = 3000;

    impl Client {

        /// Search method, receives parameters such as index, queryDsl, from, size, etc., and call the searchAPI of elasticsearch to query data. 
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-your-data.html 
        /// 
        /// # Parameters
        /// 
        /// * `index`: 
        /// The index name for query, can use wildcards, e.g. my-index-*. This will be placed in search request path. 
        /// For lower versions of elasticsearch, it can contains type, e.g. my-index/my-type. 
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html 
        /// 
        /// * `query`: 
        /// Query Dsl for query, this is a json formatted string, e.g. {"match_all":{}}. 
        /// This will be placed in the "query" field of the request body. 
        /// If not specified, the search will return all documents in the index. 
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html 
        /// 
        /// * `source`: 
        /// source filter for query, e.g. ["column1", "column2", "obj1.*", "obj2.*" ]. 
        /// This will be placed in the "_source" field of the request body. 
        /// If not specified, The query will return fields based on the default settings of the index. 
        /// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-fields.html
        /// 
        /// * `sort`: 
        /// The sort field for query, e.g. "id". This will be placed in the "sort" field of the request body. 
        /// In order to implement fast large from parameter query, the queried data must be a well-ordered set. 
        /// All the documents to be queried must have at least one unique number field, which is a numeric type and stores the 
        /// unique number of each document. The available range of the number is the entire long integer, which can be negative 
        /// and discontinuous, but the number of each document must not be repeated. 
        /// When performing fast from query, a unique number field must be used as sorting. 
        /// 
        /// * `asc`: 
        /// Sort order of the unique number field, if true, means ascending, if false, means descending.  
        /// 
        /// * `from`: 
        /// Starting document offset, how many documents to skip. a non-negative number. e.g. 100000000 
        /// Using this client, you can use very large from parameter without changing the default max_result_window setting of the index. 
        /// 
        /// * `size`: 
        /// The number of hits to return. a non-negative number. e.g. 1000000 
        /// Using this client, you can use large value parameter without changing the default max_result_window setting of the index. 
        /// 
        /// # Return
        /// A list of all documents that match the query. Each document is a json formatted string, you can choose your favorite json deserializer to parse it. 
        /// If no documents match the query, an empty list is returned. 
        /// 
        /// # Examples
        /// 
        /// ```
        /// let result = client.search(
        ///     "test_data_*", 
        ///     "{\"match_all\":{}}",
        ///     Some(&vec!["*"]), 
        ///     "id", 
        ///     true, 
        ///     100000000, 
        ///     10000).await;
        /// ```
        pub async fn search(&self, index: &str, query: &str, source: Option<&Vec<&str>>, sort: &str, asc: bool, from: i64, size: i64) -> Result<Vec<String>, Error> {
            
            // validate parameters
            if index.is_empty() {
                return Err(Error::Message(String::from("index can not be empty.")));
            }
            if sort.is_empty() {
                return Err(Error::Message(String::from("sort can not be empty.")));
            }
            if from < 0 || size < 0 {
                return Err(Error::Message(String::from("from and size can not be negative.")));
            }
            if size == 0 {
                return Ok(vec![]);
            }
            
            let query = if query == "" {"{\"match_all\":{}}"} else {query};
            let mut asc = asc;
            let mut from = from;
            let mut size = size;

            // When the queried data is near the end of the data set, reverse the query direction.
            let mut reverse = false;
            if from > MAX_FROM {
                let total = self.count(index, query).await?;
                if total == 0 || from > total {
                    return Ok(vec![]);
                }
                reverse = from > (total - from);
                if reverse {
                    asc = !asc;
                    let from2 = total - from - size;
                    let size2 = if from2 < 0 {size + from2} else {size};
                    from = from2.max(0);
                    size = size2.max(0);
                    if size == 0 {
                        return Ok(vec![]);
                    }
                }
            }

            // When the from parameter is large, find a sort value that can exclude some of the from data, and reduce the from value.
            let mut new_query = String::from(query);
            let mut new_from = from;
            if from > MAX_FROM {
                let min_item = self.query(index, query, Some(&vec![sort]), sort, true, 0, 1).await?;
                let min_item = min_item.get_hits()?.last();
                let sort_min = match min_item {
                    Some(item) => item.find_json("\"_source\"")?.find_json(&format!("\"{}\"", sort))?.get_string()?.parse::<i64>().unwrap(),
                    None => return Ok(vec![]),
                };
                let max_item = self.query(index, query, Some(&vec![sort]), sort, false, 0, 1).await?;
                let max_item = max_item.get_hits()?.last();
                let sort_max = match max_item {
                    Some(item) => item.find_json("\"_source\"")?.find_json(&format!("\"{}\"", sort))?.get_string()?.parse::<i64>().unwrap(),
                    None => return Ok(vec![]),
                };

                let new_start;
                if asc {
                    (new_start, new_from) = self.find_new_from(index, query, sort, sort_min, sort_max, from).await?;
                    new_query = Self::build_cmp_query(query, sort, "gt", new_start);
                } else {
                    (new_start, new_from) = self.find_new_from(index, query, sort, sort_max, sort_min, from).await?;
                    new_query = Self::build_cmp_query(query, sort, "lt", new_start)
                }
            }

            let mut remain_size = size;
            let mut retrieve_size = size.min(MAX_SIZE);
            let mut batch = self.query(index, &new_query, source, sort, asc, new_from, retrieve_size).await?;
            let mut hits = batch.get_hits()?;
            if hits.len() == 0 {
                return Ok(vec![]);
            }
            let mut list = vec![];
            list.extend(hits.iter().map(|item| EsJsonAnalyzer::to_json(item)));
            remain_size -= hits.len() as i64;
            while remain_size > 0 {
                let last_item = hits.last().unwrap();
                let last_sort = last_item.find_json("\"sort\"")?.get_array()?.first().unwrap().get_string()?.parse::<i64>().unwrap();
                if asc {
                    new_query = Self::build_cmp_query(query, sort, "gt", last_sort);
                } else {
                    new_query = Self::build_cmp_query(query, sort, "lt", last_sort);
                }
                retrieve_size = remain_size.min(MAX_SIZE);
                batch = self.query(index, &new_query, source, sort, asc, 0, retrieve_size).await?;
                hits = batch.get_hits()?;
                if hits.len() == 0 {
                    break;
                }
                list.extend(hits.iter().map(|item| EsJsonAnalyzer::to_json(item)));
                remain_size -= hits.len() as i64;
            }

            // If result is reverse query data, reverse it back.
            if reverse {
                list.reverse();
            }

            Ok(list)
        }

        /// Call elasticsearch's searchAPI to get the documents that meet the conditions. 
        async fn query(&self, index: &str, query: &str, source: Option<&Vec<&str>>, sort: &str, asc: bool, from: i64, size: i64) -> Result<EsJson, Error> {

            let url = format!("{}/_search", index);

            let mut query_builder = String::new();
            query_builder.push_str("{");
            query_builder.push_str(&format!("\"query\":{},", query));
            query_builder.push_str(&format!("\"sort\":{{\"{}\":\"{}\"}},", sort, if asc { "asc" } else { "desc"}));
            match source {
                Some(source) => {
                    let source_str = source.iter().map(|s|format!("\"{}\"", s)).collect::<Vec<String>>().join(",");
                    query_builder.push_str(&format!("\"_source\": [{}],", source_str));
                },
                None => {},
            }
            query_builder.push_str(&format!("\"from\":{},", from));
            query_builder.push_str(&format!("\"size\":{} }}", size));

            let body = query_builder;
            let resp = self.post(&url, &body).await?;
            let json = EsJsonAnalyzer::from_json(&resp);

            Ok(json)
        }

        /// Use binary search to find new query parameters with the same result as the original query but with a smaller from value. 
        async fn find_new_from(&self, index: &str, query: &str, sort: &str, sort_start: i64, sort_end: i64, from: i64) -> Result<(i64, i64), Error> {
            let mut new_start = sort_start;
            let mut new_end = sort_end;
            let mut new_from: i64;
            loop {
                let sort_min = new_start.min(new_end);
                let sort_abs = (new_end - new_start).abs();
                if sort_abs <= 1 {
                    return Ok((sort_min, sort_abs));
                }
                let sort_mid = sort_min + sort_abs / 2;

                let mid_query: String;
                if sort_start < sort_end {
                    mid_query = Self::build_range_query(query, sort, sort_start, sort_mid);
                } else {
                    mid_query = Self::build_range_query(query, sort, sort_mid, sort_start);
                }
                let mid_count = self.count(index, &mid_query).await?;
                new_from = from - mid_count;

                if new_from < 0 {
                    new_end = sort_mid;
                } else {
                    new_start = sort_mid;
                    if new_from <= MAX_FROM {
                        break;
                    }
                }
            }

            Ok((new_start, new_from))
        }

        /// Add range restrictions to the original query. 
        fn build_range_query(query: &str, sort: &str, start: i64, end: i64) -> String {
            format!("{{\"bool\":{{\"must\":{},\"filter\":{{\"range\":{{\"{}\":{{\"gte\":{},\"lte\":{}}}}}}}}}}}", query, sort, start, end)
        }

        /// Add range restrictions to the original query. 
        fn build_cmp_query(query: &str, sort: &str, cmp: &str, value: i64) -> String {
            format!("{{\"bool\":{{\"must\":{},\"filter\":{{\"range\":{{\"{}\":{{\"{}\":{}}}}}}}}}}}", query, sort, cmp, value)
        }

        /// Call elasticsearch's countAPI to get the total number of documents that meet query conditions. 
        async fn count(&self, index: &str, query: &str) -> Result<i64, Error> {
            let url = format!("{}/_count", index);
            let body = format!("{{\"query\": {}}}", query);
            let resp = self.post(&url, &body).await?;
            let json = EsJsonAnalyzer::from_json(&resp);
            let value = json.find_json("\"count\"")?.get_string()?;
            match value.parse::<i64>() {
                Ok(count) => Ok(count),
                Err(e) => Err(Error::Message(format!("Parse error: {}", e))),
            }
        }

        /// Call elasticsearch low level rest client, post json to elasticsearch cluster. 
        async fn post(&self, url: &str, body: &str) -> Result<String, Error> {

            let resp = self.0
                .send(
                    elasticsearch::http::Method::Post,
                    url,
                    HeaderMap::new(),
                    Option::<&str>::None,
                    Some(body),
                    None,
                ).await;

            match resp {
                Ok(resp) if resp.status_code() != 200  => {
                    let err = String::from_utf8(resp.bytes().await.unwrap().to_vec()).unwrap_or_default();
                    Err(Error::Message(err))
                }
                Err(e) => {
                    Err(Error::Message(format!("{}", e)))
                }
                _ => {
                    Ok(String::from_utf8(resp.unwrap().bytes().await.unwrap().to_vec()).unwrap_or_default())
                }
            }
        }
    }

    /// json struct
    enum EsJson {
        Array(Vec<EsJson>),
        Object(Vec<(String, EsJson)>),
        String(String),
    }

    /// json struct view functions
    impl EsJson {
        fn get_array(&self) -> Result<&Vec<EsJson>, Error> {
            match self {
                EsJson::Array(arr) => Ok(arr),
                _ => Err(Error::Message(String::from("invalid json"))),
            }
        }

        fn get_object(&self) -> Result<&Vec<(String, EsJson)>, Error> {
            match self {
                EsJson::Object(obj) => Ok(obj),
                _ => Err(Error::Message(String::from("invalid json"))),
            }
        }

        fn get_string(&self) -> Result<&String, Error> {
            match self {
                EsJson::String(s) => Ok(s),
                _ => Err(Error::Message(String::from("invalid json"))),
            }
        }

        fn find_json(&self, key: &str) -> Result<&EsJson, Error> {
            let obj = self.get_object()?;
            match obj.iter().find(|i| i.0 == key) {
                Some((_, v)) => Ok(v),
                None => Err(Error::Message(String::from("invalid json"))),
            }
        }

        fn get_hits(&self) -> Result<&Vec<EsJson>, Error> {
            let hits = self.find_json("\"hits\"")?.find_json("\"hits\"")?.get_array()?;
            Ok(hits)
        }
    }

    /// Simple json analyzer. 
    /// In order to keep dependencies low, use this own json analyzer. 
    struct EsJsonAnalyzer {
        json: Vec<char>,
        length: usize,
        position: usize,
        character: char,
    }

    impl EsJsonAnalyzer {

        /// Create a json analyzer.
        fn new(str: &str) -> EsJsonAnalyzer {
            let chars = str.chars().collect::<Vec<char>>();
            let pos = 0;
            EsJsonAnalyzer {
                position: pos,
                character: chars[pos],
                length: chars.len(),
                json: chars,
            }
        }

        /// Goto next json character.
        fn goto_next_char(&mut self) -> bool {
            self.position += 1;
            if self.position < self.length {
                self.character = self.json[self.position];
                return true;
            }
            false
        }

        /// Count the number of consecutive occurrences of a specified character before the current character
        fn count_prev_char(&self, prev_char: char) -> usize {
            let mut char_count = 0;
            let mut prev_pos = self.position - 1;

            loop {
                let char = self.json[prev_pos];
                if char == prev_char {
                    char_count += 1;
                } else {
                    break;
                }
                prev_pos -= 1;
                if prev_pos == 0 {
                    break;
                }
            }
            char_count
        }

        /// Skip space characters.
        fn skip_space(&mut self) -> i32 {
            let mut skip_count = 0;
            const SPACE_CHARS: [char; 4] = [' ', '\t', '\n', '\r'];
            while SPACE_CHARS.contains(&self.character) {
                if !self.goto_next_char() {
                    break;
                }
                skip_count += 1;
            }
            skip_count
        }

        /// Read a json array.
        fn read_json_array (&mut self) -> Vec<EsJson> {
            assert_eq!(self.character, '[');
            self.goto_next_char();
            let mut ary = vec![];
            let mut last_pos : usize = 0;
            loop {
                assert_ne!(self.position, last_pos);
                last_pos = self.position;

                self.skip_space();

                if self.character == ',' {
                    self.goto_next_char();
                    continue;
                }
                if self.character == ']' {
                    self.goto_next_char();
                    break;
                }
                ary.push(self.read_json_value());
            }
            ary
        }

        /// Read a json value.
        fn read_json_value(&mut self) -> EsJson {
            match self.character {
                '\"' => EsJson::String(self.read_json_string()),
                '{' => EsJson::Object(self.read_json_object()),
                '[' => EsJson::Array(self.read_json_array()),
                _ => EsJson::String(self.read_json_literal())
            }
        }

        /// Read a json object.
        fn read_json_object (&mut self) -> Vec<(String, EsJson)> {
            assert_eq!(self.character, '{');
            self.goto_next_char();
            let mut obj = vec![];
            let mut last_pos = 0;
            loop {
                assert_ne!(self.position, last_pos);

                last_pos = self.position;
                self.skip_space();
                if self.character == ',' {
                    self.goto_next_char();
                    continue;
                }
                if self.character == '}' {
                    self.goto_next_char();
                    break;
                }
                let key_value = self.read_json_key_value();
                obj.push(key_value);
            }
            obj
        }

        /// Read a json literal.
        fn read_json_literal(&mut self) -> String {
            const STOP_CHARS: [char; 7] = [' ', '\t', '\r', '\n', ',', ']', '}'];
            let mut literal = String::new();
            loop {
                if STOP_CHARS.contains(&self.character) {
                    break;
                }
                literal.push(self.character);
                if !self.goto_next_char() {
                    break;
                }
            }
            literal
        }

        /// Read a json string.
        fn read_json_string(&mut self) -> String {
            assert_eq!(self.character, '\"');
            let mut string = String::from(self.character);
            while self.goto_next_char() {
                string.push(self.character);
                if self.character == '\"' && self.count_prev_char('\\') % 2 == 0 {
                    self.goto_next_char();
                    break;
                }
            }
            string
        }

        /// Read a json key-value pair.
        fn read_json_key_value(&mut self) -> (String, EsJson) {
            self.skip_space();
            assert_eq!(self.character, '\"');
            let key = self.read_json_string();
            self.skip_space();
            assert_eq!(self.character, ':');
            self.goto_next_char();
            self.skip_space();
            (key, self.read_json_value())
        }

        /// Deserialize json string to EsJson.
        fn from_json(json: &str) -> EsJson {
            EsJsonAnalyzer::new(json).read_json_value()
        }

        /// Serialize EsJson to json string.
        fn to_json(obj: &EsJson) -> String {
            match obj {
                EsJson::Array(ary) => {
                    let ary_str = ary.iter().map(|v| Self::to_json(v)).collect::<Vec<String>>().join(",");
                    return format!("[{}]", ary_str);
                }
                EsJson::Object(obj) => {
                    let obj_str = obj.iter().map(|(k, v)| format!("{}:{}", k, Self::to_json(v))).collect::<Vec<String>>().join(",");
                    return format!("{{{}}}", obj_str);
                }
                EsJson::String(str) => {
                    return format!("{}", str);
                }
            }
        }
    }
}


