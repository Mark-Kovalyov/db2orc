#!/bin/bash -v

mkdir release

xml tr xsl/transform.xsl -s mainClassName="mayton.db.Orc2Db" template | tee pom.xml && \
    mvn clean package && \
    cp target/db2orc*.jar release/orc2db.jar



