#!/bin/bash

# Get article names from see also output

if [ "$#" -ne 3 ]; then
    echo "Error: Illegal number of parameters"
    echo "USAGE: sh prepare_mlt_queries.sh <seealso-csv> <clickstream-tsv> <output>"
    exit 1
fi

# Arguments
SEEALSO_CSV=$1
CLICKSTREAM_TSV=$2
OUTPUT=$3

#CLICKSTREAM_COL=4

cat $CLICKSTREAM_TSV | awk '{print $4}' | sed -e 's/_/ /g' > $OUTPUT
gawk 'BEGIN{RS="\n"; FS="|"}{print $1}' $SEEALSO_CSV >> $OUTPUT
cat $OUTPUT | sort | uniq > $OUTPUT

echo done

exit 0