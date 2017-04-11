#!/usr/bin/env bash

# --relative-proximity

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