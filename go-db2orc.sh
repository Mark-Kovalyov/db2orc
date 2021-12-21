#!/bin/bash -v

value=$1

export JAVA_HOME=/jdk/11

java -Xmx2G \
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  --orc.compression ZLIB \
  -o "$value-ZLIB.orc" \
  -t "$value"
