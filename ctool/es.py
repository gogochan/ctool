import urllib3
import progressbar
from typing import List

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


def client_factory(host, **kwargs) -> Elasticsearch:
    """Get Elasticsearch client."""
    common_args = {"hosts": [host], "verify_certs": False, "ssl_show_warn": False}

    if "username" in kwargs and "password" in kwargs:
       return Elasticsearch(basic_auth=(kwargs.get("username"), kwargs.get("password")), **common_args)
    elif "api_key" in kwargs:
       return Elasticsearch(api_key=kwargs.get("api_key"), **common_args)
    raise ValueError("Either username/password or api_key must be provided")


def get_all_indices(client:Elasticsearch) -> List[str]:
    """Get all indices from Elasticsearch."""
    resp = client.indices.get_alias()
    return resp.keys()

def get_all_data_streams(client:Elasticsearch) -> List[str]:
    """Get all data_streams from Elasticsearch."""
    resp = client.indices.get_data_stream()
    return [val["name"] for val in resp.body["data_streams"]]
