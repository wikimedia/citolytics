#!/bin/bash

# mlt_queries: 3 225 355
########  res: 3 208 731

#  1) 0 - 1m (10x100k)
#  2) 1m - 2m
#  3) 2m - 2.1m (10x10k)
#  4) 2.1m - 2.6m
#  5) 2.6 - 3.3

#     500000
#     750000
#START=0
#SIZE=100000

#START=1000000

START=2600000
# 2600000
SIZE=100000

#    20000
# filter = (path or nofilter)
FILTER=/home/mschwarzer/mlt_queries
CLASS=de.tuberlin.dima.mschwarzer.lucene.morelikethis.ResultCollector
JAR=/home/mschwarzer/wiki2lucene/wiki.jar

for i in {1..7}
do
	let END=$START+$SIZE
	echo "start $START - $END out: $i"

	# single line output format
	nohup java -cp $JAR $CLASS dima-power-cluster ibm-power-1.dima.tu-berlin.de wikipedia article mlt_results_$START.csv $FILTER $START $END y  &> t$i.out &

	let START=$END
done