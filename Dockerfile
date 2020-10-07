FROM ubuntu:18.04
RUN apt install openjdk-11-jdk-headless
COPY db2orc.jar        /opt/db2orc/
COPY orc2db.jar        /opt/db2orc/
COPY db2orc.properties /opt/db2orc/
ENTRYPOINT ["/usr/bin/java"]
CMD ["-jar", "/opt/spring-cloud/lib/spring-cloud-config-server.jar"]
VOLUME /var/lib/spring-cloud/config-repo
