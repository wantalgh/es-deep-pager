import { HttpConnection, ClusterConnectionPool, Transport } from "@elastic/transport";
import DeepPageClient from "../src/DeepPageClient";

var schame: string = "http";
var hosts: string = "node1:9200,node2:9200,node3:9200";

var useranme: string = "elastic";
var password: string = "search";

var pool = new ClusterConnectionPool({
    Connection: HttpConnection,
    auth: { username: useranme, password: password },
});
hosts.split(",").forEach(host => pool.addConnection(schame + "://" + host));

var transport = new Transport({
    connectionPool: pool,
});
var client = new DeepPageClient(transport);

var response = client.search(
    "test_data_*", 
    "{\"match_all\": {}}",
    ["*"],
    "id",
    true,
    100000000,
    10000
);

response
    .then(result => console.log(result.length))
    .catch(error => console.log(error));
