

use elasticsearch::http::Url;
use es_deep_pager::deep_page_client;
use elasticsearch::http::transport::{Connection, ConnectionPool, Transport, TransportBuilder};
use elasticsearch::auth::Credentials;
use rand::Rng;


#[tokio::main]
async fn main() {

    let transport = build_es_transport();
    let client = deep_page_client::Client(transport);
    let result = client.search(
        "test_data_*", 
        "",
        Option::None, 
        "id", 
        true, 
        10000, 
        5000).await;

    match result {
        Ok(v) => {
            println!("length: {}", v.len());
        }
        Err(deep_page_client::Error::Message(s)) => {
            println!("error: {}", s);
        }        
    }
}

#[derive(Debug, Clone)]
struct MultiNodePool{
    connections: Vec<Connection>
}

impl MultiNodePool {
    fn build(schame: &str, hosts: &str) -> MultiNodePool{
        let nodes = hosts.split(",")
            .map(|s|Url::parse(format!("{}://{}", schame, s).as_str()).unwrap())
            .map(|u|Connection::new(u))
            .collect::<Vec<Connection>>();
        MultiNodePool { connections : nodes }
    }
}

impl ConnectionPool for MultiNodePool {
    fn next(&self) -> &Connection {
        let index = rand::thread_rng().gen_range(0..self.connections.len());
        self.connections.get(index).unwrap()
    }
}


fn build_es_transport() -> Transport {

    let schame = "http";
    let hosts = "node1:9200,node2:9200,node3:9200";
    let username = "elastic";
    let password = "search";

    let conn_pool = MultiNodePool::build(schame, hosts);
    let transport = TransportBuilder::new(conn_pool)
        .auth(Credentials::Basic(username.into(), password.into()))
        .build().unwrap();
    transport
}