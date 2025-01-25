import urllib3
import progressbar

from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def walk_data(client:Elasticsearch, index: str, chunk_size: int, show_progress: bool = True):
    """Dump data from indicies on Elasticsearch
    """
    try:
        _resp = client.search(index=index, body={"query": {"match_all": {}}, "size": chunk_size}, scroll='1m')

    except Exception as e:
        print(f"Error: {e}")

    def _generator(resp):
        while len(resp['hits']['hits']) > 0:
            for _doc in resp['hits']['hits']:
                yield _doc
            try:
                resp = client.scroll(scroll_id=resp['_scroll_id'], scroll='1m')
            except Exception as e:
                print(f"Error: {e}")
                break

    if show_progress:
        return progressbar.progressbar(
            _generator(_resp),
            max_value=_resp["hits"]["total"]["value"])
    else:
        return _generator(_resp)


def get_client(host, **kwargs) -> Elasticsearch:
    if "username" in kwargs and "password" in kwargs:
       return Elasticsearch(hosts=[host], basic_auth=(kwargs.get("username"), kwargs.get("password")), verify_certs=False)
    elif "api_key" in kwargs:
       return Elasticsearch(hosts=[host], api_key=kwargs.get("api_key"), verify_certs=False) 
    raise ValueError("Either username/password or api_key must be provided")
