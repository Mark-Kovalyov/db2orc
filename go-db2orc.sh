#!/bin/bash -v

# General test

# Debug
#  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \

java -version

for value in organization
do
 java -Xmx2G \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="./dump" \
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "$value.orc" \
  -t "$value" \
  -co ZLIB \
  --fetchsize 80 \
  --batchsize 950 \
  -ri 0

 if [ -f "$value.orc" ]; then
    orc-metadata -v "$value.orc" > "$value-metadata.txt"
 fi

done

