# CirrusSearch

How to generate results that can be populated to the Elasticsearch index of Wikipedia's CirrusSearch extension.


## Everything at once
```


```

### With prepared data
``` 
export FLINK_HOME=
export WIKI_DUMP=
export DATA_DIR=hdfs://cluster

# Extract redirects
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.redirects.RedirectExtractor --input $WIKI_DUMP \
    --output $DATA_DIR/redirects

# Extract id-mapping


```