call mvn clean test

call mvn clean package -Dtargetbinary=db2orc -DskipTests
xcopy target\db2orc-*.jar db2orc.jar

call mvn clean package -Dtargetbinary=orc2db -DskipTests
xcopy target\db2orc-*.jar orc2db.jar



