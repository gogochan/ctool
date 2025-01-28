## Overview
A simple tool for dumping and inspecting Elasticsearch data.

### Installation
```
git clone https://github.com/gogochan/ctool
cd ctool
python -m venv venv
source venv/bin/activate
pip install .
```

### dump
```
ctool dump -h
Usage: ctool dump [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit.

Commands:
  datastream  Dump data streams from Elasticsearch to a folder.
  index       Dump indices from Elasticsearch to a folder.
```

This subcommand has `index` and `datastream` subcommand for retrieving data from
Elasticsearch. The usage is almost identical.

The user credentials can be specified using environment variables.

```
ELASTICSEARCH_HOST=xxx
ELASTICSEARCH_USER=xxx
ELASTICSEARCH_PASSWORD=xxx
ELASTICSEARCH_API_KEY=xxx
```


#### index
```
ctool dump index -h
Usage: ctool dump index [OPTIONS] TARGET_FOLDER

  Dump indices from Elasticsearch to a folder.

Options:
  --host TEXT                 Elasticsearch host
  --username TEXT             Elasticsearch username
  --password TEXT             Elasticsearch password  [required]
  --api-key TEXT              Elasticsearch API key, if specified, will be
                              used instead of username/password
  --index TEXT                Elasticsearch index to dump, if not specified,
                              the program will try all indices
  --chunk-size INTEGER        Number of documents to download, default is 200
  --checksum / --no-checksum  Store checksum for each document in a separate
                              file
  -h, --help                  Show this message and exit.
```

This command download documents from Elasticsearch indices. If no index is specified,
the tool attempts to download from all indices.

By default, the tool writes hash of each document in a separated file. This behavior can
be disabled using `--no-checksum`

#### datastream
```
ctool dump datastream -h
Usage: ctool dump datastream [OPTIONS] TARGET_FOLDER

  Dump data streams from Elasticsearch to a folder.

Options:
  --host TEXT                 Elasticsearch host
  --username TEXT             Elasticsearch username
  --password TEXT             Elasticsearch password  [required]
  --api-key TEXT              Elasticsearch API key, if specified, will be
                              used instead of username/password
  --data-stream TEXT          Elasticsearch data streams to dump, if not
                              specified, the program will try all data_streams
  --chunk-size INTEGER        Number of documents to download, default is 200
  --checksum / --no-checksum  Store checksum for each document in a separate
                              file
  -h, --help                  Show this message and exit.
```

### compare

```
Usage: ctool compare [OPTIONS] LEFT RIGHT

  Compare two Elasticsearch index dump data stored in different directory.

  For this to work, dump must have been run with --checksum option.

Options:
  -h, --help  Show this message and exit.

```
This command is meant to compare two dump results using hash.


### load

This command upload documents to Elasticsearch

```
Usage: ctool load [OPTIONS] DATA_FILES...

  Load data into Elasticsearch.

  The source_files must be in nsjson format.

Options:
  --host TEXT           Elasticsearch host
  --username TEXT       Elasticsearch username
  --password TEXT       Elasticsearch password  [required]
  --api-key TEXT        Elasticsearch API key, if specified, will be used
                        instead of username/password
  --target TEXT         Elasticsearch index or data stream  [required]
  --chunk-size INTEGER  Number of documents to download, default is 200
  --pipeline TEXT       Elasticsearch ingest pipeline
  -h, --help            Show this message and exit.
```