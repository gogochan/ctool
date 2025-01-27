import click
import os
import json

from ctool.es import  walk_data, get_client, get_all_data_streams
import hashlib

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--host", envvar="ELASTICSEARCH_HOST", default="localhost", help="Elasticsearch host")
@click.option("--username", envvar="ELASTICSEARCH_USERNAME", default="elastic", help="Elasticsearch username")
@click.option("--password", envvar="ELASTICSEARCH_PASSWORD", required=True, help="Elasticsearch password")
@click.option("--api-key", envvar="ELASTICSEARCH_API_KEY", help="Elasticsearch API key, if specified, will be used instead of username/password")
@click.option("--index", "indicies", multiple=True, help="Elasticsearch index to dump, if not specified, the program will try all data_streams")
@click.option("--chunk-size", default=200, help="Number of documents to download, default is 200")
@click.option("--store-checksum", default=False, is_flag=True, help="Store checksum for each document in a separate file")
@click.argument("target_folder")
def dump(host, username, password, api_key, indicies, chunk_size, target_folder, store_checksum):
    """Dump data from Elasticsearch to a folder.
    """
    click.echo(f"Creating target folder: {target_folder}")
    os.makedirs(target_folder, exist_ok=True)

    click.echo("Dumping data...")
    client = get_client(host, username=username, password=password, api_key=api_key)

    if not indicies:
        indicies = get_all_data_streams(client)

    for index in indicies:
        click.echo(f"Processing index: {index}...")
        buf = { doc["_id"]: doc["_source"] for doc in walk_data(client, index, chunk_size) }

        with open(os.path.join(target_folder, f"{index}.json"), "w") as f:
            json.dump(buf, f, indent=4)

        if store_checksum:
            with open(os.path.join(target_folder, f"{index}-checksum.json"), "w") as f:
                json.dump({k: hashlib.sha256(json.dumps(v).encode()).hexdigest() for k, v in buf.items()}, f)
    

@click.group(context_settings=CONTEXT_SETTINGS)
def es():
    pass

@click.group(context_settings=CONTEXT_SETTINGS)
def root():
    pass

es.add_command(dump)
root.add_command(es)

def main():
    root()
