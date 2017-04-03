Flink Jobs
================================

You run Flink jobs from this repository by using the following commands. Degree of parallelism (-p) depends on cluster setup.

### Setup Environment

    export SPARK_HOME=/share/spark-1.6.0-bin-hadoop2.4
    export FLINK_HOME=/share/flink/current
    export HDFS_PATH=hdfs://ibm-power-1.dima.tu-berlin.de:44000
    export JAR=/home/mschwarzer/citolytics/target/cpa-0.1.jar
    export PARALLELISM=150
        
    export WIKI="enwiki"
    export WIKI_DUMP=$HDFS_PATH/user/mschwarzer/$WIKI/input/$WIKI-20170101-pages-articles.xml
    export INTERMEDIATE_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/intermediate
    export OUTPUT_DIR=$HDFS_PATH/user/mschwarzer/$WIKI/output
    export ENWIKI_LANGLINKS=$HDFS_PATH/user/mschwarzer/enwiki/input/enwiki-20170101-langlinks.sql
    export ENWIKI_IDTITLE_MAPPING=$HDFS_PATH/user/mschwarzer/enwiki/intermediate/idtitle
    export CLICKSTREAMS_PATH=$HDFS_PATH/user/mschwarzer/gold/clickstream
    
### WikiSim (no redirects)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --alpha 0.855 \
        --output $OUTPUT_DIR/wikisim_raw
        
### WikiSim (with prepared-redirects; alpha = {0.855}; no thresholds)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --redirects $INTERMEDIATE_DIR/redirects \
        --alpha 0.855 \
        --output $OUTPUT_DIR/wikisim_raw
        
### WikiSim (with redirects on-the-fly)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --resolve-redirects \
        --alpha 0.855 \
        --output $OUTPUT_DIR/wikisim_raw
        
### SeeAlsoEvaluation

#### CPA
```
flink run -p 64 -c SeeAlsoEvaluation \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/results/a01_redirected \
    hdfs:///user/mschwarzer/v2/results/seealso_cpa_1_0 \
    hdfs:///user/mschwarzer/v2/intermediate/seealso_redirects \
    nofilter 8
```

#### MLT
```
flink run -p 82 -c SeeAlsoEvaluation \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/intermediate/mlt_results \
    hdfs:///user/mschwarzer/v2/results/seealso_mlt \
    hdfs:///user/mschwarzer/v2/intermediate/seealso2_redirected \
    nofilter -1
```

### ClickStreamEvaluation

#### CPA
   
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.clickstream.ClickStreamEvaluation -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --gold $CLICKSTREAMS_PATH \
        --topk 10 \
        --output $OUTPUT_DIR/clickstream
            
    # With language links (simplewiki translated from enwiki)
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.clickstream.ClickStreamEvaluation -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --gold $CLICKSTREAMS_PATH \
        --topk 10 \
        --langlinks $ENWIKI_LANGLINKS \
        --lang simple \
        --output $OUTPUT_DIR/clickstream
        
    # With language links, id-title mapping, summary (simplewiki translated from enwiki)
        $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.clickstream.ClickStreamEvaluation -p $PARALLELISM $JAR \
            --wikisim $OUTPUT_DIR/wikisim_raw \
            --gold $CLICKSTREAMS_PATH \
            --topk 10 \
            --id-title-mapping $ENWIKI_IDTITLE_MAPPING \
            --langlinks $ENWIKI_LANGLINKS \
            --lang simple \
            --summary \
            --output $OUTPUT_DIR/clickstream
             
       
```
flink run -p 96 -c ClickStreamEvaluation \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/results/a01_redirected \
    hdfs:///datasets/enwiki_2015_02_clickstream.tsv \
    hdfs:///user/mschwarzer/v2/results/clickstream_cpa_c \
    5
```

#### MLT
```
flink run -p 96 -c ClickStreamEvaluation \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/intermediate/mlt_results \
    hdfs:///datasets/enwiki_2015_02_clickstream.tsv \
    hdfs:///user/mschwarzer/v2/results/clickstream_mlt_c \
    -1
```

### Redirects

#### Resolve Redirects (from WikiSim output)

```
flink run -p 96 -c WikiSimRedirects \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/results/a01 \
    hdfs:///user/mschwarzer/v2/intermediate/redirects \
    hdfs:///user/mschwarzer/v2/results/a01_redirected
```

#### Extract Redirects (from WikiDump)

```
./bin/flink run -c RedirectExtractor \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///user/mschwarzer/v2/intermediate/redirects2 \
```

#### Replace redirects in "See Also" links
```
flink run -p 64 -c SeeAlsoRedirects \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/intermediate/seealso6e.csv \
    hdfs:///user/mschwarzer/v2/intermediate/redirects \
    hdfs:///user/mschwarzer/v2/intermediate/seealso_redirects
```

### Misc

#### Test WikiSim Output Integrity (Debugging)
```
flink run -p 96 -c CheckOutputIntegrity \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/tests/a01_redirected \
    hdfs:///user/mschwarzer/v2/tests/a01b_redirected \
    hdfs:///user/mschwarzer/v2/tests/integrity \
```

#### Stats (words, headlines, outLinks, avgLinkDistance, outLinksPerWords, inLinks)
```
flink run -p 96 -c ArticleStatsWithInboundLinks \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///user/mschwarzer/v2/results/stats2
```

With resolved redirects for inLinks:

```
flink run -p 96 -c ArticleStatsWithInboundLinks \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///user/mschwarzer/v2/results/stats2
    hdfs:///user/mschwarzer/v2/intermediate/redirects
```

#### Get detailed link graph (debugging cpa rankings)

[WIKI DATASET] [REDIRECTS] [LINKTUPLE CSV] [OUTPUT]

```
flink run -p 96 -c LinkGraph \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///user/mschwarzer/v2/intermediate/redirects \
    file:///share/flink/mschwarzer/linkgraph.in \
    hdfs:///user/mschwarzer/v2/linkgraph.out
```    
    

    
  