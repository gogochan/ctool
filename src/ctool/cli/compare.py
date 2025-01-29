import click
import glob
import json
import os
import progressbar
from progressbar.terminal.colors import red, green
from .context import CONTEXT_SETTINGS

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("left")
@click.argument("right")
def compare(left, right):
    """Compare two Elasticsearch data stored in different directory.

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

