## Overview
A simple tool for dumping and inspecting Elasticsearch data.

### es

### dump
```
Usage: ctool es dump [OPTIONS] TARGET_FOLDER

  Dump data from Elasticsearch to a folder.

Options:
  --host TEXT                 Elasticsearch host
  --username TEXT             Elasticsearch username
  --password TEXT             Elasticsearch password  [required]
  --api-key TEXT              Elasticsearch API key, if specified, will be
                              used instead of username/password
  --index TEXT                Elasticsearch index to dump, if not specified,
                              the program will try all data_streams
  --chunk-size INTEGER        Number of documents to download, default is 200
  --checksum / --no-checksum  Store checksum for each document in a separate
                              file
  -h, --help                  Show this message and exit.

```

This command download documents from Elasticsearch indices. If no index is specified,
the tool attempts to download from all data streams.

By default, the tool writes hash of each document in a separated file. This behavior can
be disabled using `--no-checksum`

The user credentials can be specified using environment variables.

```
ELASTICSEARCH_HOST=xxx
ELASTICSEARCH_USER=xxx
ELASTICSEARCH_PASSWORD=xxx
ELASTICSEARCH_API_KEY=xxx
```


### compare

```
Usage: ctool es compare [OPTIONS] LEFT RIGHT

  Compare two Elasticsearch index dump data stored in different directory.

  For this to work, dump must have have been run with --checksum option.

Options:
  -h, --help  Show this message and exit.

```
This command is meant to compare two dump results using hash.
