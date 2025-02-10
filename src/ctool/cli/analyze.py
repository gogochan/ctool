import click
import glob
import json
import os
import hashlib

import progressbar
from progressbar.terminal.colors import red, green

from .context import CONTEXT_SETTINGS


def del_attributes(doc: dict, dotted_exclude: str) -> dict:
    """Delete attributes from a dictionary using dotted notation"""
    if "." in dotted_exclude:
        key, dotted_exclude = dotted_exclude.split(".", maxsplit=1)
        if key in doc:
            doc[key] = del_attributes(doc[key], dotted_exclude)
    else:
        if dotted_exclude in doc:
            del doc[dotted_exclude]

    return doc


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--exclude", "excludes", multiple=True, help="Additional fields to exclude when comparing")
@click.argument("target_folder")
def duplicate(excludes: list[str], target_folder: str):
    """Walk through dump of Elasticsearch and report possible duplicates.

    By default, the tool ignore event.id, event.created_at, and event.updated_at fields.

    If any additional fields need to be excluded, use --exclude option.
    """

    _excludes = [
        "event.id",
        "event.created_at",
        "event.updated_at",
        *excludes,
    ]

    hash_set = {}

    for f in glob.glob(os.path.join(target_folder, "*.json")):
        # Skip checksum files
        if f.endswith("-checksum.json"):
            continue

        filename = os.path.basename(f)
        index_name = filename[:-len(".json")]

        click.echo(f"Processing {index_name}...")

        with open(f) as f1:
            f1.readline()  # skip the first line
            for line in f1:
                if ":" not in line:
                    break

                _id, val = line.split(":", maxsplit=1)
                doc = json.loads(val.rstrip(",\n"))
                doc_id = _id.strip('"')

                for exclude in _excludes:
                    del_attributes(doc, exclude)
                doc_hash = hashlib.sha256(json.dumps(doc).encode()).hexdigest()

                if doc_hash in hash_set:
                    click.echo(
                        red.fg(f"Duplicate found: {doc_id} in {index_name} and {hash_set[doc_hash]}"))
                else:
                    hash_set[doc_hash] = (index_name,  doc_id,)


@click.group(context_settings=CONTEXT_SETTINGS)
def analyze():
    """Group of command dumping data from Elasticsearch to a folder."""


analyze.add_command(duplicate)
