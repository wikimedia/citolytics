# How to debug Citolytics recommendations

(use env vars from cirrussearch.md)

```
export DEBUG_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/debug

#### Extract links from XML dump
#### $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.linkgraph.LinksExtractor -p $PARALLELISM $JAR \
####    $WIKI_DUMP $DEBUG_DIR/links

# Define links tuples you would like to debug (save to linkgraph.in)

1608|Footballer
1608|1607
Afternoon of a Faun (Nijinsky)|Ballet
...

# Extract link graph for link tuples
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.linkgraph.LinkGraph -p $PARALLELISM $JAR \
    $WIKI_DUMP $INTERMEDIATE_DIR/redirects $DEBUG_DIR/linkgraph.in $DEBUG_DIR/linkgraph.out
    

# Get CSV output from HDFS
$HADOOP_HOME/bin/hdfs dfs -getmerge $DEBUG_DIR/linkgraph.out $WIKI_linkgraph.out

# Analyze output CSV (manully re-compute scores)

```