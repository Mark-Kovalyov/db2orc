#!/bin/bash -v

mkdir release
xml tr xsl/transform.xsl -s mainClassName="mayton.db.Db2Orc" template | tee pom.xml

mvn -X clean package 2>&1 | tee mvn.log
mvn dependency:tree -Dverbose 2>&1 | tee dependency-conflicts.log

cp target/db2orc*.jar release/db2orc.jar




