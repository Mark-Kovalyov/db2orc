call mvn clean package 

echo F | xcopy /Y target\orc2db*.jar release\orc2db.jar

