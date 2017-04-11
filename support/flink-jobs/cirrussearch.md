# CirrusSearch

How to generate results that can be populated to the Elasticsearch index of Wikipedia's CirrusSearch extension? First
load env vars. Then, use "Everything at once" command to compute all results on the fly or use HDFS as storage of intermediate
results.

### env vars

    export SPARK_HOME=/share/spark-1.6.0-bin-hadoop2.4
    export FLINK_HOME=/share/flink/current
    export HDFS_PATH=hdfs://ibm-power-1.dima.tu-berlin.de:44000
    export WIKI="enwiki"
    export WIKI_DUMP=$HDFS_PATH/user/mschwarzer/$WIKI/input/$WIKI-20170101-pages-articles.xml
    export INTERMEDIATE_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/intermediate
    export OUTPUT_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/output
    export JAR=/home/mschwarzer/citolytics/target/cpa-0.1.jar
    export PARALLELISM=150

### Everything at once

    # Creates ES dump file
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cirrussearch.PrepareOutput -p $PARALLELISM $JAR \
        --wikidump $WIKI_DUMP \
        --enable-elastic \
        --resolve-redirects \
        --output $OUTPUT_DIR/citolytics_$WIKI.json
    
    # Ignore missing id
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cirrussearch.PrepareOutput -p $PARALLELISM $JAR \
        --wikidump $WIKI_DUMP \
        --enable-elastic \
        --ignore-missing-ids \
        --resolve-redirects \
        --output $OUTPUT_DIR/wikisim_elastic_ignore_missing_ids.json

### With prepared data

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
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --enable-elastic \
        --output $OUTPUT_DIR/citolytics_$WIKI.json

### Alternative pre-processing options

    # Generate top-10 results as JSON
    # - with CPI expression and article stats, e.g.
    #   --cpi %1\$f*Math.log\(%3\$d/%2\$d\)  
    # - WARNING: expression needs to be escaped
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cirrussearch.PrepareOutput -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --enable-elastic \
        --cpi x*log\(z\/\(y+1\)\) \
        --article-stats $OUTPUT_DIR/stats \
        --output $OUTPUT_DIR/citolytics_$WIKI.cpi.json

### Trigger ES import

    curl -s -XPOST 'http://localhost:9200/mediawiki_content_first/page/_bulk' -d @wikisim.json
