# https://www.howtoforge.com/tutorial/how-to-install-oracle-database-12c-on-centos-7/#step-install-required-packages

FROM centos:7

RUN yum install -y binutils.x86_64 compat-libcap1.x86_64 gcc.x86_64 gcc-c++.x86_64 glibc.i686 glibc.x86_64 \
 glibc-devel.i686 glibc-devel.x86_64 ksh compat-libstdc++-33 libaio.i686 libaio.x86_64 libaio-devel.i686 libaio-devel.x86_64 \
 libgcc.i686 libgcc.x86_64 libstdc++.i686 libstdc++.x86_64 libstdc++-devel.i686 libstdc++-devel.x86_64 libXi.i686 libXi.x86_64 \
 libXtst.i686 libXtst.x86_64 make.x86_64 sysstat.x86_64

RUN groupadd oinstall
RUN groupadd dba
RUN useradd -g oinstall -G dba oracle
RUN passwd oracle

/etc/sysctl.conf

fs.aio-max-nr = 1048576
fs.file-max = 6815744
kernel.shmall = 2097152
kernel.shmmax = 2147483648
kernel.shmmni = 4096
kernel.sem = 250 32000 100 128
net.ipv4.ip_local_port_range = 9000 65500
net.core.rmem_default = 262144
net.core.rmem_max = 4194304
net.core.wmem_default = 262144
net.core.wmem_max = 1048586

RUN sysctl -p
RUN sysctl -a

#/etc/security/limits.conf

oracle soft nproc 2047
oracle hard nproc 16384
oracle soft nofile 1024
oracle hard nofile 65536

yum groupinstall -y "X Window System"

# Download linuxamd64_12102_database_se2_1of2.zip

RUN yum -y install zip unzip

# Mount external folder with Oracle distr

RUN unzip linuxamd64_12102_database_se2_1of2.zip -d /stage/
RUN unzip linuxamd64_12102_database_se2_2of2.zip -d /stage/

RUN chown -R oracle:oinstall /stage/

RUN mkdir -p /u01 /u02

RUN chown -R oracle:oinstall /u01 /u02
RUN chmod -R 775 /u01 /u02
RUN chmod g+s /u01 /u02

export ORACLE_SID=orcl
export ORACLE_HOME=/u01/app/oracle/product/12.1.0/dbhome_1/
export PATH=$PATH:$ORACLE_HOME/bin

#sqlplus / as sysdba

#alter user sys identified by yourpassword;