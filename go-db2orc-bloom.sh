#!/bin/bash -v

# General test
#  -s "select id,family_name,given_name from person where family_name is not null" \


for value in person
do
 java -Xmx2G \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="./dump" \
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "/storage/db2orc/bloom/$value.orc" \
  -t "$value" \
  --orc.bloomcolumns "family_name,id" \
  --orc.compression "NONE"

 if [ -f "/storage/db2orc/bloom/$value.orc" ]; then
    orc-metadata -v            "/storage/db2orc/bloom/$value.orc" > "/storage/db2orc/bloom/$value-metadata.txt"
    orc-scan                   "/storage/db2orc/bloom/$value.orc" > "/storage/db2orc/bloom/$value-scan.txt"
    orc-statistics --withIndex "/storage/db2orc/bloom/$value.orc" > "/storage/db2orc/bloom/$value-statistics.txt"
    orc-tools data             "/storage/db2orc/bloom/$value.orc" > "/storage/db2orc/bloom/$value.json"
 fi

done
