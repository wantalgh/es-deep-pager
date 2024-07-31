package wantalgh.esdeeppager;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class DeepPageClientTest  {

    /**
     * build deep page client
     */
    private DeepPageClient buildDeepPageClient() {
        RestClient restClient = buildRestClient();
        DeepPageClient pageClient = new DeepPageClient(restClient);
        return pageClient;
    }

    /**
     * test deep paging search
     */
    @Test
    public void test() throws IOException {

        // init deep page client
        DeepPageClient pageClient = buildDeepPageClient();

        // call search method
        List<String> results = pageClient.search(
                "test_data_*",
                null,
                null,
                "pre_number",
                true,
                100000000,
                10000);

        System.out.println(results.size());
    }

    /**
     * build elasticsearch low level rest client
     */
    private RestClient buildRestClient() {

        String scheme = "http";
        String hosts = "node1:9200,node2:9200,node3:9200";

        String username = "elastic";
        String password = "search";
        String auth = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());

        HttpHost[] httpHosts = Arrays.stream(hosts.split(","))
                .map(s -> s.split(":"))
                .map(s -> new HttpHost(s[0], Integer.parseInt(s[1]), scheme))
                .toArray(HttpHost[]::new);

        RestClient restClient = RestClient
                .builder(httpHosts)
                .setDefaultHeaders(new Header[]{new BasicHeader("Authorization", "Basic " + auth)})
                .build();
        return restClient;
    }
}
