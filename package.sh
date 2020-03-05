#!/bin/bash -v

sed "s/mainClassName/mayton.db.Db2Orc/g" template | sed "s/artifactName/db2orc/g" > pom.xml

mvn clean package 

cp target/db2orc*.jar release/db2orc.jar

sed "s/mainClassName/mayton.db.Orc2Db/g" template | sed "s/artifactName/orc2db/g" > pom.xml

mvn clean package 

cp target/orc2db*.jar release/orc2db.jar

