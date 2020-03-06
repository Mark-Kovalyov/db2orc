#!/bin/bash -v

mkdir release

sed "s/mainClassName/mayton.db.Db2Orc/g" template > pom.xml

mvn clean package 

cp target/db2orc*.jar release/db2orc.jar

sed "s/mainClassName/mayton.db.Orc2Db/g" template > pom.xml

mvn clean package 

cp target/db2orc*.jar release/orc2db.jar

