call mvn clean package -Dtargetbinary=db2orc -DskipTests
del /F db2orc.jar
echo F | xcopy target\db2orc-*.jar db2orc.jar

java -jar db2orc.jar %1 %2 %3 %4 %5 %6 %7 %8


