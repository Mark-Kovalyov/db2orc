#!/bin/bash -v

java -jar db2orc.jar \
 -u "jdbc:postgresql://127.0.0.1:5432/demo" \
 -l mayton \
 -p ********* \
 -o bookings.orc \
 -t bookings
