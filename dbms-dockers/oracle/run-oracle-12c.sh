#!/bin/bash

docker run -p 1521:1521 \
 -v ./u01:/u01 \
 -v ./u02:/u02 \
 -e ORACLE_SID=orcl \
 -e ORACLE_HOME=/u01/app/oracle/product/12.1.0/dbhome_1/ \
 -e PATH=$PATH:$ORACLE_HOME/bin