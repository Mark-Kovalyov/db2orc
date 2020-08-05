#!/bin/bash -v

#mvn clean package

#cp shared/target/shared-1.0-SNAPSHOT.jar db2orc-cli/target/ 

mvn dependency:copy -Dartifact='commons-cli:commons-cli:1.4'

cp target/dependency/commons-cli-1.4.jar db2orc-cli/target/

java -jar db2orc-cli/target/db2orc-cli-1.0-SNAPSHOT.jar