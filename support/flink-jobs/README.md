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
    export LANGLINKS=$HDFS_PATH/user/mschwarzer/$WIKI/input/$WIKI-20170101-langlinks.sql
    export ENWIKI_LANGLINKS=$HDFS_PATH/user/mschwarzer/enwiki/input/enwiki-20170101-langlinks.sql
    export ENWIKI_IDTITLE_MAPPING=$HDFS_PATH/user/mschwarzer/enwiki/intermediate/idtitle
    export CLICKSTREAMS_PATH=$HDFS_PATH/user/mschwarzer/gold/clickstream
    export SEEALSO_PATH=$HDFS_PATH/user/mschwarzer/gold/seealso
        
    
### WikiSim 

(no redirects)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --alpha 0.855 \
        --output $OUTPUT_DIR/wikisim_raw
        
(with prepared-redirects; alpha = {0.855}; no thresholds)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --redirects $INTERMEDIATE_DIR/redirects \
        --alpha 0.855 \
        --output $OUTPUT_DIR/wikisim_raw
        
(with redirects on-the-fly)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --resolve-redirects \
        --alpha 0.855 \
        --output $OUTPUT_DIR/wikisim_raw

        
(with structure proximity + redirects)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --resolve-redirects \
        --alpha 1.0 \
        --structure-proximity \
        --output $OUTPUT_DIR/wikisim_structure_raw
        
(relative proximity)

        $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim -p $PARALLELISM $JAR \
            --input $WIKI_DUMP \
            --resolve-redirects \
            --alpha 1.0 \
            --relative-proximity \
            --output $OUTPUT_DIR/wikisim_1_0_relative_raw
    
        
        
### SeeAlsoEvaluation

Evaluate WikiSim output (CPA)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.seealso.SeeAlsoEvaluation  -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --gold $SEEALSO_PATH \
        --output $OUTPUT_DIR/seealso


Evaluate MoreLikeThis output (MLT)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.seealso.SeeAlsoEvaluation  -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/mlt \
        --gold $SEEALSO_PATH \
        --output $OUTPUT_DIR/seealso_mlt

### ClickStreamEvaluation

Evaluate WikiSim output (CPA)
   
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
        
    # With CPI expression
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.clickstream.ClickStreamEvaluation -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --gold $CLICKSTREAMS_PATH \
        --topk 10 \
        --id-title-mapping $ENWIKI_IDTITLE_MAPPING \
        --langlinks $ENWIKI_LANGLINKS \
        --lang simple \
        --summary \
        --article-stats $OUTPUT_DIR/stats \
        --cpi x*log\(z\/\(y+1\)\) \
        --output $OUTPUT_DIR/cs_cpi
        
    
    # With CPI expression + top recommendations
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.clickstream.ClickStreamEvaluation -p $PARALLELISM $JAR \
        --wikisim $OUTPUT_DIR/wikisim_raw \
        --gold $CLICKSTREAMS_PATH \
        --topk 10 \
        --id-title-mapping $ENWIKI_IDTITLE_MAPPING \
        --langlinks $ENWIKI_LANGLINKS \
        --lang simple \
        --summary \
        --article-stats $OUTPUT_DIR/stats \
        --cpi x*log\(z\/\(y+1\)\) \
        --top-recommendations $OUTPUT_DIR/top \
        --output $OUTPUT_DIR/cs_cpi
        
### EditEvaluation
     
Evaluate recommendations based on edit history dumps

    # Simple evaluation
    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.edits.EditRecommendationExtractor -p $PARALLELISM $JAR \
        --input $HDFS_PATH/user/mschwarzer/$WIKI/input/$WIKI-20170520-stub-meta-history.xml \
        --output $HDFS_PATH/user/mschwarzer/$WIKI/output/edit_recommendations
       
#### MLT
```
flink run -p 96 -c ClickStreamEvaluation \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///user/mschwarzer/v2/intermediate/mlt_results \
    hdfs:///datasets/enwiki_2015_02_clickstream.tsv \
    hdfs:///user/mschwarzer/v2/results/clickstream_mlt_c \
    -1
```

## Helper jobs

The following jobs perform pre-processing or analysis tasks.

### SeeAlsoExtractor

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.seealso.SeeAlsoExtractor  -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --output $SEEALSO_PATH
        
With multi-language translation (See [cirrusearch.md](cirrussearch.md) for lang-links extraction)

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.seealso.SeeAlsoExtractor  -p $PARALLELISM $JAR \
        --input $WIKI_DUMP \
        --input-lang de \
        --lang-links $LANGLINKS \
        --output $SEEALSO_PATH.$WIKI

                        

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

    $FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.stats.ArticleStats -p $PARALLELISM $JAR \
        --wikidump $WIKI_DUMP \
        --in-links \
        --output $OUTPUT_DIR/stats


#### CPI Sampler (retrieves a random portion of CPI values)

```
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.stats.CPISampler -p $PARALLELISM $JAR \
    --input $OUTPUT_DIR/wikisim_raw \
    --output $OUTPUT_DIR/cpi_sample_0_1 \
    --p 0.1
```

### CPI Analysis (extract data for analysing CPI values)

```
# articles: Force,New York City,Brad Pitt,Nachos,British Asian,Einstein field equations,Daft Pink,Elizabeth II
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.stats.CPIAnalysis  -p $PARALLELISM $JAR \
    --top-k 3 --articles "Force,New York City,Brad Pitt,Nachos,British Asian,Einstein field equations,Daft Pink,Elizabeth II" \
    --wikisim $OUTPUT_DIR/wikisim_raw \
    --clickstream $CLICKSTREAMS_PATH \
    --id-title-mapping $ENWIKI_IDTITLE_MAPPING \
    --langlinks $ENWIKI_LANGLINKS \
    --lang simple \
    --stats $OUTPUT_DIR/stats \
    --output $OUTPUT_DIR/cpi_analysis \
    --clickstream-output $OUTPUT_DIR/cpi_analysis_cs
    
# hdfs dfs -getmerge $OUTPUT_DIR/cpi_analysis tmp/$WIKI_cpi_analysis
# hdfs dfs -get $OUTPUT_DIR/cpi_analysis_cs tmp/$WIKI_cpi_analysis_cs

$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.stats.CPIAnalysis  -p $PARALLELISM $JAR \
    --top-k 3 --articles "Force,New York City,Brad Pitt,Nachos,British Asian,Einstein field equations,Daft Pink,Elizabeth II" \
    --wikisim $OUTPUT_DIR/wikisim_raw_relative \
    --clickstream $CLICKSTREAMS_PATH \
    --id-title-mapping $ENWIKI_IDTITLE_MAPPING \
    --langlinks $ENWIKI_LANGLINKS \
    --lang simple \
    --stats $OUTPUT_DIR/stats \
    --output $OUTPUT_DIR/cpi_analysis_rel \
    --clickstream-output $OUTPUT_DIR/cpi_analysis_cs_rel

# hdfs dfs -getmerge $OUTPUT_DIR/cpi_analysis_rel tmp/$WIKI_cpi_analysis_rel
# hdfs dfs -get $OUTPUT_DIR/cpi_analysis_cs_rel tmp/$WIKI_cpi_analysis_cs_rel

    
```

### Link distance sampler 

```
$FLINK_HOME/bin/flink run -c org.wikipedia.citolytics.stats.LinkDistanceSampler  -p $PARALLELISM $JAR \
    --input $WIKI_DUMP \
    --p 0.1 \
    --stats $OUTPUT_DIR/stats \
    --output $OUTPUT_DIR/cpi_analysis_rel \
    --output $OUTPUT_DIR/linkdistance
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
    

    
  