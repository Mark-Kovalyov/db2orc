#!/bin/bash -v

mkdir release

xml tr xsl/param1.xsl -s mainClassName=mayton.db.Db2Orc out.xml

mvn clean package

cp target/db2orc*.jar release/db2orc.jar




