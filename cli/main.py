import click
import glob
import json
import os
import progressbar
from progressbar.terminal.colors import green, red
from ctool.es import  walk_data, client_factory, get_all_data_streams
from ctool.data import _progressive_write

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--host", envvar="ELASTICSEARCH_HOST", default="localhost", help="Elasticsearch host")
@click.option("--username", envvar="ELASTICSEARCH_USERNAME", default="elastic", help="Elasticsearch username")
@click.option("--password", envvar="ELASTICSEARCH_PASSWORD", required=True, help="Elasticsearch password")
@click.option("--api-key", envvar="ELASTICSEARCH_API_KEY", help="Elasticsearch API key, if specified, will be used instead of username/password")
@click.option("--index", "indicies", multiple=True, help="Elasticsearch index to dump, if not specified, the program will try all data_streams")
@click.option("--chunk-size", default=200, help="Number of documents to download, default is 200")
@click.option("--checksum/--no-checksum", "store_checksum", default=True, help="Store checksum for each document in a separate file")
@click.argument("target_folder")
def dump(host, username, password, api_key, indicies, chunk_size, target_folder, store_checksum):
    """Dump data from Elasticsearch to a folder.
    """
    click.echo(f"Creating target folder: {target_folder}")
    os.makedirs(target_folder, exist_ok=True)

    click.echo("Dumping data...")
    client = client_factory(host, username=username, password=password, api_key=api_key)

    if not indicies:
        indicies = get_all_data_streams(client)

    for index in indicies:
        click.echo(f"Processing index: {index}...")

        _itr = walk_data(client, index, chunk_size)

        if store_checksum:
            with open(os.path.join(target_folder, f"{index}.json"), "w") as f, \
                 open(os.path.join(target_folder, f"{index}-checksum.json"), "w") as f_checksum:
                _progressive_write(_itr, f, f_checksum)
        else:
            with open(os.path.join(target_folder, f"{index}.json"), "w") as f:
                _progressive_write(_itr, f)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("left")
@click.argument("right")
def compare(left, right):
    """Compare two Elasticsearch index dump data stored in different directory.

    For this to work, dump must have been run with --checksum option.
    """

    click.echo(f"Comparing {left} and {right}...")
    suffix = "-checksum.json"
    for f in glob.glob(os.path.join(left, "*-checksum.json")):
        filename = os.path.basename(f)
        index_name = filename[:-len(suffix)]

        click.echo(f"Processing {index_name}...")

        with open(f) as f1:
            left_data = json.load(f1)
        try:
            with open(os.path.join(right, filename)) as f2:
                right_data = json.load(f2)
        except FileNotFoundError as e:
            click.echo(red.fg(f"Index {index_name} is missing in {right}"))
            continue

        for doc_id, doc_hash in progressbar.progressbar(left_data.items(), max_value=len(left_data)):
            if doc_id not in right_data:
                click.echo(red.fg(f"Document {doc_id} is missing in {right}"))
            elif doc_hash != right_data[doc_id]:
                click.echo(red.fg(f"Document {doc_id} is different in {right}"))
        else:
            click.echo(green.fg(f"All documents from {index_name} are identical in {left} and {right}"))
            green.get_color


@click.group(context_settings=CONTEXT_SETTINGS)
def es():
    pass

@click.group(context_settings=CONTEXT_SETTINGS)
def root():
    pass

es.add_command(dump)
es.add_command(compare)
root.add_command(es)

def main():
    root()
