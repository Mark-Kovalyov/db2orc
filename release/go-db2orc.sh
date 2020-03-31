#!/bin/bash -v

DEMO_DB=**
DEMO_USER=**
DEMO_PWD=**

MTNDB_DB=**
MTNDB_USER=**
MTNDB_PWD=**

rm -f *crc
rm -f *orc
rm -f *txt
rm -f *log

# General test

for value in aircrafts_data airports_data boarding_passes bookings flights seats ticket_flights tickets
do
 java -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "$value.orc" \
  -t "$value" \
  -co ZLIB \
  -ri 0 \
  2>&1 | tee "$value.log"

 if [ -f "$value.orc" ]; then
    orc-metadata -v "$value.orc" > "$value-metadata.txt"
    orc-contents -v "$value.orc" > "$value-contents.txt"
 fi

done

# Test SNAPPY compression

for value in geolite_ipv4 geolite_ipv6 geolite_loc
do
 java -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$MTNDB_DB" \
  -l $MTNDB_USER \
  -p $MTNDB_PWD \
  -o "$value.orc" \
  -t "$value" \
  -co SNAPPY \
  -ri 0 \
  -ss 33554432 \
  2>&1 | tee "$value.log"

 if [ -f "$value.orc" ]; then
    orc-metadata -v "$value.orc" > "$value-metadata.txt"
    orc-contents -v "$value.orc" > "$value-contents.txt"
 fi

done

# Test BLOOM-filter for specific column set

for value in chastniki
do
 java -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$MTNDB_DB" \
  -l $MTNDB_USER \
  -p $MTNDB_PWD \
  -o "$value.orc" \
  -t "$value" \
  -co NONE \
  -ri 0 \
  -bc id,raion,adr,tel,datar,pasps,paspn,wd,fam,im,otch \
  -bf 0.97 \
  2>&1 | tee "$value.log"

 if [ -f "$value.orc" ]; then
    orc-metadata -v "$value.orc" > "$value-metadata.txt"
    orc-contents -v "$value.orc" > "$value-contents.txt"
 fi

done
