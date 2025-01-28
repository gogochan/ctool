import click
from .dump import dump
from .compare import compare
from .load import load
from .context import CONTEXT_SETTINGS


@click.group(context_settings=CONTEXT_SETTINGS)
def root():
    pass


root.add_command(compare)
root.add_command(dump)
root.add_command(load)


def main():
    root()
