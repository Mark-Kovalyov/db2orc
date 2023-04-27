dest="$(HOME)/bin"

makeall: make-db2orc make-orc2db
	echo "OK"

make-db2orc:
	mvn clean package -Dtargetbinary=db2orc -DskipTests
	cp -v target/db2orc-*.jar db2orc.jar

make-orc2db:
	mvn clean package -Dtargetbinary=orc2db -DskipTests
	cp -v target/db2orc-*.jar orc2db.jar

install: makeall
	cp -f orc2db.jar $(dest)
	cp -f db2orc.jar $(dest)


