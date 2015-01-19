#!/bin/sh
/share/hadoop/schubotz/flink-0.7.0-incubating/bin/start-cluster.sh && \
/share/hadoop/schubotz/flink-0.7.0-incubating/bin/flink run \
-p 180 -c de.tuberlin.dima.schubotz.cpa.WikiSim \
/share/hadoop/schubotz/cpa-0.0.1.jar \
 hdfs://cloud-11.dima.tu-berlin.de:60010/datasets/enwiki-latest-pages-meta-current.xml \
 hdfs://cloud-11.dima.tu-berlin.de:60010/user/hadoop/physikerwelt/output/malte/1 \
 0,0.5,1.0,1.1,1.2,1.3,1.4,1.5 0 0 && \
 /share/hadoop/schubotz/flink-0.7.0-incubating/bin/start-cluster.sh
