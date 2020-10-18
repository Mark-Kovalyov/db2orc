#!/bin/bash -v

# General test

TABLE=organization

for value in NONE ZLIB SNAPPY LZO LZ4 ZSTD
do
 java -Xmx2G \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="./dump" \
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "/storage/db2orc/$TABLE-$value.orc" \
  -t "$TABLE" \
  --orc.compression "$value"

 if [ -f "/storage/db2orc/$TABLE-$value.orc" ]; then
    orc-metadata -v "/storage/db2orc/$TABLE-$value.orc" > "/storage/db2orc/$TABLE-$value-metadata.txt"
 fi

done
