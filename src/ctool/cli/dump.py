import click
import os
from progressbar.terminal.colors import red
from ctool.es import  walk_data, client_factory, get_all_data_streams, get_all_indices
from ctool.data import _progressive_write
from .context import CONTEXT_SETTINGS


def _dump_data(client, indices, chunk_size, target_folder, store_checksum):
    """Dump data from indicies on Elasticsearch"""
    click.echo(f"Creating target folder: {target_folder}")
    os.makedirs(target_folder, exist_ok=True)

    for index in indices:
        click.echo(f"Processing: {index}...")

        try:
            _itr = walk_data(client, index, chunk_size)
        except Exception as e:
            click.echo(red.fg(f"Error: {e}"))
            continue

        if store_checksum:
            with open(os.path.join(target_folder, f"{index}.json"), "w") as f, \
                 open(os.path.join(target_folder, f"{index}-checksum.json"), "w") as f_checksum:
                _progressive_write(_itr, f, f_checksum)
        else:
            with open(os.path.join(target_folder, f"{index}.json"), "w") as f:
                _progressive_write(_itr, f)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--host", envvar="ELASTICSEARCH_HOST", default="localhost", help="Elasticsearch host")
@click.option("--username", envvar="ELASTICSEARCH_USERNAME", default="elastic", help="Elasticsearch username")
@click.option("--password", envvar="ELASTICSEARCH_PASSWORD", help="Elasticsearch password")
@click.option("--api-key", envvar="ELASTICSEARCH_API_KEY", help="Elasticsearch API key, if specified, will be used instead of username/password")
@click.option("--index", "indicies", multiple=True, help="Elasticsearch index to dump, if not specified, the program will try all indicies")
@click.option("--chunk-size", default=200, help="Number of documents to download, default is 200")
@click.option("--checksum/--no-checksum", "store_checksum", default=True, help="Store checksum for each document in a separate file")
@click.argument("target_folder")
def index(host, username, password, api_key, indicies, chunk_size, target_folder, store_checksum):
    """Dump indices from Elasticsearch to a folder.
    """
    client = client_factory(host, username=username, password=password, api_key=api_key)

    if not indicies:
        indicies = get_all_indices(client)

    _dump_data(client, indicies, chunk_size, target_folder, store_checksum)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--host", envvar="ELASTICSEARCH_HOST", default="localhost", help="Elasticsearch host")
@click.option("--username", envvar="ELASTICSEARCH_USERNAME", default="elastic", help="Elasticsearch username")
@click.option("--password", envvar="ELASTICSEARCH_PASSWORD", help="Elasticsearch password")
@click.option("--api-key", envvar="ELASTICSEARCH_API_KEY", help="Elasticsearch API key, if specified, will be used instead of username/password")
@click.option("--data-stream", "data_streams", multiple=True, help="Elasticsearch data streams to dump, if not specified, the program will try all data_streams")
@click.option("--chunk-size", default=200, help="Number of documents to download, default is 200")
@click.option("--checksum/--no-checksum", "store_checksum", default=True, help="Store checksum for each document in a separate file")
@click.argument("target_folder")
def datastream(host, username, password, api_key, data_streams, chunk_size, target_folder, store_checksum):
    """Dump data streams from Elasticsearch to a folder.
    """
    client = client_factory(host, username=username, password=password, api_key=api_key)

    if not data_streams:
        data_streams = get_all_data_streams(client)

    _dump_data(client, data_streams, chunk_size, target_folder, store_checksum)


@click.group(context_settings=CONTEXT_SETTINGS)
def dump():
    """Group of command dumping data from Elasticsearch to a folder."""


dump.add_command(datastream)
dump.add_command(index)
