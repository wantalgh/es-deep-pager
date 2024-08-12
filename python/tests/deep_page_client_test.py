
from elastic_transport import Transport
from elastic_transport.client_utils import url_to_node_config
from es_deep_pager.deep_page_client import DeepPageClient


def build_deep_page_client():
    """ Build DeepPageClient. """
    schame = "http"
    hosts = "node1:9200,node2:9200,node3:9200"

    username = "elastic"
    password = "search"

    node_urls = [ f"{schame}://{username}:{password}@{host}" for host in hosts.split(",") ]
    nodes = [ url_to_node_config(url) for url in node_urls ]
    transport = Transport(nodes)
    return transport


def test_deep_page_client():
    """ Test DeepPageClient. """
    transport = build_deep_page_client()
    client = DeepPageClient(transport)
    response = client.search(
        "test_data_*", 
        "{\"match_all\":{}}", 
        ["*"], 
        "id", 
        True, 
        100000000, 
        10000)
    print(response)

