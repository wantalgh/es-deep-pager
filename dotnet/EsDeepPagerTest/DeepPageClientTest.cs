using Elastic.Transport;
using Wantalgh.EsDeepPager;


namespace Wantalgh.EsDeepPagerTest
{
    [TestClass] 
    public class DeepPageClientTest
    {
        /// <summary>
        /// test deep paging search
        /// </summary>
        [TestMethod]
        public void TestSearch()
        {
            var transport = BuildTransport();
            var client = new DeepPageClient(transport);
            var result = client.Search(
                index: "test_data_*",
                query: "{\"match_all\":{}}",
                source: ["*"],
                sort: "id",
                asc: true,
                from: 100000000,
                size: 10000    
            );
            Assert.IsNotNull(result);   
        }

        /// <summary>
        /// build low rest client
        /// </summary>
        private ITransport BuildTransport()
        {
            string hosts = "localhost:9200,localhost:9200,localhost:9200";
            string username = "elastic";
            string password = "search";

            Node[] nodes = hosts.Split(',').Select(x => new Node(new Uri($"http://{x}"))).ToArray();
            NodePool pool = new StaticNodePool(nodes);
            TransportConfiguration settings = new TransportConfiguration(pool)
                .Authentication(new BasicAuthentication(username, password));
            ITransport transport = new DistributedTransport(settings);

            return transport;
        }
    }
}
