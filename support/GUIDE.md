# Guide

## Cluster Setup

- Install Flink v1.1+ with Hadoop

## Data

- Download Wikipedia Dump from
- Unzip
- Push to HDFS

```
wget $PATH_TO_WIKIPEDIA_XML_DUMP -O dump.tar
untar dump.tar
hdfs -put dump.tar $HDFS_PATH
```

## Prepare data

See [cirrussearch.md](cirrussearch.md)

## Write to Elasticsearch

```
# Fetch from HDFS
hdfs -getmerge $HDFS_PATH $DIR/data/citolytics.json

# Split into batches
mkdir $DIR/data/citolytics.splits.d
split -l 1000 $DIR/data/citolytics.json $DIR/data/citolytics.splits.d/

# Send each batch to ES
for f in $DIR/data/citolytics.splits.d/{.,}*; do curl -XPOST localhost:9200/mediawiki_content_first/page/_bulk?pretty --data-binary @$f; done

```
