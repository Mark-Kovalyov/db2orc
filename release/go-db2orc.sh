#!/bin/bash -v

java -jar db2orc.jar \
 -u "jdbc:postgresql://127.0.0.1:5432/pgdb" \
 -l pguser \
 -p pgpwd123 \
 -o geolite_ipv4.orc \
 -t geolite_ipv4
