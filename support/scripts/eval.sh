#!/bin/bash

OUTPUT_FILENAME=$1
#TOPK=$2
CPA_KEY=$2

PARALLELISM="150"

MAIN_CLASS="de.tuberlin.dima.schubotz.wikisim.cpa.evaluation.Evaluation"
HDFS_URL="hdfs://ibm-power-1.dima.tu-berlin.de:8020"
JAR_PATH="/home/mschwarzer/wikisim/cpa.jar"

SEEALSO_DATASET="user/mschwarzer/wikisim/eval/seealso6e.csv"
xxWIKISIM_DATASET="user/mschwarzer/wikisim/results-0-2_5-5"
#WIKISIM_DATASET="user/mschwarzer/wikisim/results-05-10_0-0"
#WIKISIM_DATASET="user/mschwarzer/wikisim/results_b"
WIKISIM_DATASET="user/mschwarzer/wikisim/results_c"

MLT_DATASET="user/mschwarzer/wikisim/eval/mlt_textonly"
LINKFILTER_DATASET=$HDFS_URL/user/mschwarzer/wikisim/links
AGGREGATE="n"

# p 150
# WikiSim Eval
#current/bin/flink run -p $PARALLELISM -c de.tuberlin.dima.schubotz.wikisim.seealso.BetterEvaluation $JAR_PATH $HDFS_URL/$WIKISIM_DATASET $HDFS_URL/$1 $HDFS_URL/$SEEALSO_DATASET $2

# MLT Eval
#current/bin/flink run -p 100 -c de.tuberlin.dima.schubotz.wikisim.seealso.BetterEvaluation $JAR_PATH $HDFS_URL/$MLT_DATASET $HDFS_URL/$1 $HDFS_URL/$SEEALSO_DATASET $LINKFILTER_DATASET 2 0 1

# CPA 0.81 (results_c:6)
current/bin/flink run -p 100 -c de.tuberlin.dima.schubotz.wikisim.cpa.evaluation.BetterEvaluation $JAR_PATH $HDFS_URL/$WIKISIM_DATASET $HDFS_URL/$1 $HDFS_URL/$SEEALSO_DATASET $LINKFILTER_DATASET 6

# CoCit

#current/bin/flink run -p 150 -c EvaluationHistogram $JAR_PATH $HDFS_URL/$WIKISIM_DATASET $HDFS_URL/user/mschwarzer/wikisim/evalperformance y
#/home/mschwarzer/wikisim/cpa.jar hdfs://ibm-power-1.dima.tu-berlin.de:8020

#current/bin/flink run -p $PARALLELISM -c $MAIN_CLASS $JAR_PATH $HDFS_URL/$OUTPUT_FILENAME $HDFS_URL/$SEEALSO_DATASET $HDFS_URL/$WIKISIM_DATASET $HDFS_URL/$MLT_DATASET $LINKFILTER_DATASET $TOPK $CPA_KEY $AGGREGATE
