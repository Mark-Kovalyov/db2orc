call mvn clean test

call mvn clean package -Dtargetbinary=db2orc -DskipTests
del /F db2orc.jar
echo F | xcopy target\db2orc-*.jar db2orc.jar

call mvn clean package -Dtargetbinary=orc2db -DskipTests
del /F orc2db.jar
echo F | xcopy target\db2orc-*.jar orc2db.jar



