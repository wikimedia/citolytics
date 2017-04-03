# Guide

Detailed information on all available Flink jobs can be found [here](support/flink-jobs/README.md). 
The following guide describes all steps to run Citolytics on a cluster and import the recommendations to MediaWiki/CirrusSearch.

## Cluster Setup

- Install Flink v1.1+ with Hadoop (HDFS)

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

See [cirrussearch.md](flink-jobs/cirrussearch.md)

## Write to Elasticsearch

MediaWiki/CirrusSearch reads the recommendations from Elasticsearch. Hence, the recommendations need to be populated to the ES index.
```
# Fetch from HDFS
hdfs -getmerge $HDFS_PATH $DIR/data/citolytics.json

# Split into batches
mkdir $DIR/data/citolytics.splits.d
split -l 1000 $DIR/data/citolytics.json $DIR/data/citolytics.splits.d/

# Send each batch to ES
for f in $DIR/data/citolytics.splits.d/{.,}*; do curl -XPOST localhost:9200/mediawiki_content_first/page/_bulk?pretty --data-binary @$f; done

```
