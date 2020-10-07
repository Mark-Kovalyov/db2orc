#!/bin/bash -v

rm -f *crc
rm -f *orc
rm -f *txt
rm -f *log

# General test

# Debug
#  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \

java -version

#                 rows     |  pages (est)   |  Disk size
# ======================================================
# person       14 383 339  |  476 093         3720 MB
# organization  6 300 010  |  179 988         1407 MB

for value in person
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
  -ri 0

 if [ -f "$value.orc" ]; then
    orc-metadata -v "$value.orc" > "$value-metadata.txt"
    orc-contents -v "$value.orc" > "$value-contents.txt"
 fi

done

