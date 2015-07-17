#!/bin/bash

HDFS_PATH=$1
FILENAME=$2
FLINK_OUTPUT_DIR="/share/flink/output/"
KEY_FILENAME="/home/mschwarzer/msmieo"

SSH_HOST="malte@md02.sciplore.net"
DB_DIR="/data/hdd3tb1/wikisim/results/"
DB_COLUMNS="article,seealso_total,seealso_links,cpa_links,cpa_links_size,cpa_hrr,cpa_topk,cpa_matches10,cpa_matches5,cpa_matches1,cocit_links,cocit_links_size,cocit_hrr,cocit_topk,cocit_matches10,cocit_matches5,cocit_matches1,mlt_links,mlt_links_size,mlt_hrr,mlt_topk,mlt_matches10,mlt_matches5,mlt_matches1"

xDB_COLUMNS="hash,page1,page2,distance,count,distSquared,min,max,cpa1,cpa2,cpa3,cpa4,cpa5,cpa6,cpa7"
xxDB_COLUMNS="article,words"
xxxDB_COLUMNS="article,inbound,outbound,words,words_quartile"
yDB_COLUMNS="article,seealso_total,seealso_links,cpa_links,cpa_links_size,cpa_hrr,cpa_topk,cpa_matches10,cocit_links,cocit_links_size,cocit_hrr,cocit_topk,cocit_matches10,mlt_links,mlt_links_size,mlt_hrr,mlt_topk,mlt_matches10"
zDB_COLUMNS="article,seealso_links,seealso_total,cpa_links,cpa_links_size,cpa_hrr,cpa_topk"
DB_COLUMNS="article,seealso_links,seealso_total,cpa_links,cpa_links_size,cpa_hrr,cpa_topk,map,cpa_matches10,cpa_matches5,cpa_matches1"
xxDB_COLUMNS="name,words,headlines,outlinks,avglinkdistance,outlinksperwords"

echo "MySQL Password:"
read DB_PASSWORD

DB_CREATE_TABLE="CREATE  TABLE  $FILENAME (  article varchar( 100  )  NOT  NULL , seealso_total int( 11  )  NOT  NULL , seealso_links text NOT  NULL , cpa_links text NOT  NULL , cpa_links_size smallint( 3  )  NOT  NULL , cpa_matches10 smallint( 5  )  NOT  NULL , cpa_matches5 int( 11  )  NOT  NULL , cpa_matches1 int( 11  )  NOT  NULL , cocit_links text NOT  NULL , cocit_links_size smallint( 3  )  NOT  NULL , cocit_matches10 smallint( 5  )  NOT  NULL , cocit_matches5 int( 11  )  NOT  NULL , cocit_matches1 int( 11  )  NOT  NULL , mlt_links text NOT  NULL , mlt_links_size smallint( 3  )  NOT  NULL , mlt_matches10 smallint( 5  )  NOT  NULL , mlt_matches5 int( 11  )  NOT  NULL , mlt_matches1 int( 11  )  NOT  NULL , PRIMARY  KEY (  article  ) , KEY  cpa_matches (  cpa_matches10 ,  cocit_matches10 ,  mlt_matches10  ) , KEY  cpa_matches5 (  cpa_matches5  ) , KEY  seealso_total (  seealso_total  ) , KEY  cpa_matches10 (  cpa_matches1  ) , KEY  cocit_matches5 (  cocit_matches5  ) , KEY  cocit_matches10 (  cocit_matches1  ) , KEY  mlt_matches5 (  mlt_matches5  ) , KEY  mlt_matches10 (  mlt_matches1  )  ) ENGINE  =  MyISAM  DEFAULT CHARSET  = utf8;"

# 1. getmerge
# 2. scp
# 3. mysqlimport

hadoop fs -getmerge $HDFS_PATH $FLINK_OUTPUT_DIR$FILENAME
scp -i $KEY_FILENAME $FLINK_OUTPUT_DIR$FILENAME malte@md02.sciplore.net:$DB_DIR$FILENAME
#echo "ssh -i $KEY_FILENAME malte@md02.sciplore.net -- \"mysqlimport --local -u malte -p wikisim --fields-terminated-by='|' --columns='$DB_COLUMNS' $DB_DIR$FILENAME"
ssh -i $KEY_FILENAME $SSH_HOST "mysql -u malte -p\"$DB_PASSWORD\" wikisim -e \"TRUNCATE TABLE $FILENAME; ALTER TABLE $FILENAME DISABLE KEYS;\";mysqlimport --local -u malte -p\"$DB_PASSWORD\" wikisim --fields-terminated-by='|' --columns='$DB_COLUMNS' $DB_DIR$FILENAME;mysql -u malte -p\"$DB_PASSWORD\" wikisim -e \"ALTER TABLE $FILENAME ENABLE KEYS;\""
#ssh -i $KEY_FILENAME $SSH_HOST "mysql -u malte -p wikisim -e \"ALTER TABLE $FILENAME DISABLE KEYS;\""
#ssh -i $KEY_FILENAME malte@md02.sciplore.net "mysqlimport --local -u malte -p wikisim --fields-terminated-by='|' --columns='$DB_COLUMNS' $DB_DIR$FILENAME"
#ssh -i $KEY_FILENAME $SSH_HOST "mysql -u malte -p wikisim -e \"ALTER TABLE $FILENAME ENABLE KEYS;\""

#echo $CREATE_TABLE

