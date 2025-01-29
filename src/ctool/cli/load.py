from functools import reduce

import click
from progressbar import progressbar
from progressbar.terminal.colors import red

from .context import CONTEXT_SETTINGS
from ctool.es import client_factory, bulk_index

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--host", envvar="ELASTICSEARCH_HOST", default="localhost", help="Elasticsearch host")
@click.option("--username", envvar="ELASTICSEARCH_USERNAME", default="elastic", help="Elasticsearch username")
@click.option("--password", envvar="ELASTICSEARCH_PASSWORD", required=True, help="Elasticsearch password")
@click.option("--api-key", envvar="ELASTICSEARCH_API_KEY", help="Elasticsearch API key, if specified, will be used instead of username/password")
@click.option("--target", required=True, help="Elasticsearch index or data stream")
@click.option("--chunk-size", default=500, help="Number of documents to download, default is 200")
@click.option("--pipeline", default=None, help="Elasticsearch ingest pipeline")
@click.argument("data_files", nargs=-1, required=True)
def load(host, username, password, api_key, target, chunk_size, pipeline, data_files):
    """Load data into Elasticsearch.

    The source_files must be in nsjson format.
    """
    client = client_factory(host, username=username, password=password, api_key=api_key)

    for data in data_files:
        click.echo(f"Loading {data} into {target}...")
        with open(data) as source_file:
            total = reduce(lambda x, _: x+1, (1 for line in source_file))
            source_file.seek(0)
            for resp in progressbar(
                bulk_index(client, target, source_file, chunk_size, pipeline),
                max_value=total):
                if not resp[0]:
                    click.echo(red.ansi(f"Error: {resp[1]}"))
