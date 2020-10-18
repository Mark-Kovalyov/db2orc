#!/bin/bash -v

java -Xmx2G \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="./dump" \
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "organization.orc" \
  -t "organization"
