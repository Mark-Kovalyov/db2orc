#!/bin/bash -v

# General test

# Debug
#  
#  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \

for value in person
do
 java -Xmx2G \
  -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="./dump" \
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "$value.orc" \
  --selectexpr "select * from $value"

 if [ -f "$value.orc" ]; then
    orc-metadata -v "$value.orc" > "$value-metadata.txt"
 fi

done

