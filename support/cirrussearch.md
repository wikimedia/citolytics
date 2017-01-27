# CirrusSearch

How to generate results that can be populated to the Elasticsearch index of Wikipedia's CirrusSearch extension.

### env vars
```
export SPARK_HOME=/share/spark-1.6.0-bin-hadoop2.4
export FLINK_HOME=/share/flink/current
export HDFS_PATH=hdfs://ibm-power-1.dima.tu-berlin.de:44000
export WIKI="enwiki"
export WIKI_DUMP=$HDFS_PATH/user/mschwarzer/$WIKI/input/$WIKI-20170101-pages-articles.xml
export INTERMEDIATE_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/intermediate
export OUTPUT_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/output
export JAR=/home/mschwarzer/citolytics/target/cpa-0.1.jar
export PARALLELISM=150
```

### Everything at once
```

$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cirrussearch.PrepareOutput -p $PARALLELISM $JAR \
    --wikidump $WIKI_DUMP \
    --output $OUTPUT_DIR/wikisim

```

### With prepared data
``` 

# Extract redirects
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.redirects.RedirectExtractor -p $PARALLELISM $JAR \
    --input $WIKI_DUMP \
    --output $INTERMEDIATE_DIR/redirects

# Extract id-mapping
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cirrussearch.IdTitleMappingExtractor -p $PARALLELISM $JAR \
    --input $WIKI_DUMP \
    --output $INTERMEDIATE_DIR/idtitle

# Generate raw results as CSV
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
    --input $WIKI_DUMP \
    --redirects $INTERMEDIATE_DIR/redirects \
    --alpha 0.855 \
    --output $OUTPUT_DIR/wikisim_raw

# Generate top-10 results as JSON
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cirrussearch.PrepareOutput -p $PARALLELISM $JAR \
    --wikidump $WIKI_DUMP \
    --idtitle-mapping $INTERMEDIATE_DIR/idtitle \
    --output $OUTPUT_DIR/wikisim
```