# db2orc + orc2db
This is an Education project to analyze apache orc abilities.

## DBMS support

|Dbms     |Supported|
|---------|---------|
|Postgres | 95%     |
|Oracle   | TBD     |
|MS-SQL   | TBD     |
|MySQL    | TBD     |
|MariaDB  | TBD     |
|DB2      | Licence?| 

## Usage

Export from database to orc:
```
$ db2orc --help
usage:
     888 888       .d8888b.
     888 888      d88P  Y88b
     888 888             888
 .d88888 88888b.       .d88P  .d88b.  888d888 .d8888b
d88" 888 888 "88b  .od888P"  d88""88b 888P"  d88P"
888  888 888  888 d88P"      888  888 888    888
Y88b 888 888 d88P 888"       Y88..88P 888    Y88b.
 "Y88888 88888P"  888888888   "Y88P"  888     "Y8888P

 -b,--batchsize <arg>             Batch size (rows) default = 50 000
 -bc,--orc.bloomcolumns <arg>     Orc file bloom filter columns
                                  (comma-separated)
 -bf,--orc.bloomfilterfpp <arg>   False positive probability (float) for
                                  bloom filter [0.75..0.99]
 -co,--orc.compression <arg>      Orc file compression := { NONE, ZLIB,
                                  SNAPPY, LZO, LZ4, ZSTD }
 -l,--login <arg>                 JDBC login
 -o,--orcfile <arg>               Orc file. (ex:big-data.orc)
 -p,--password <arg>              JDBC password
 -ri,--orc.rowindexstride <arg>   Row index stride [0..1000], 0 - means no
                                  index will be.
 -s,--selectexpr <arg>            SELECT-expression (ex: SELECT * FROM
                                  EMP)
 -ss,--orc.stripesize <arg>       The writer stores the contents of the
                                  stripe in memory until this memory limit
                                  is reached and the stripe is flushed to
                                  the HDFS file and the next stripe
                                  started
 -t,--tablename <arg>             Table or View name
 -u,--url <arg>                   JDBC url.
                                  (ex:jdbc:oracle:thin@localhost:1521/XE
 
```

## Usecase 1: Export table 'organization' from PG database to ORC
```
java -Xmx2G \  
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "organization.orc" \
  -t "organization"
```
## Usecase 1: Export table 'person' from PG database to ORC with column selection
```
java -Xmx2G \  
  -jar db2orc.jar \
  -u "jdbc:postgresql://127.0.0.1:5432/$DEMO_DB" \
  -l $DEMO_USER \
  -p $DEMO_PWD \
  -o "person.orc" \
  --selectexpr "select id,given_name from person where given_name is not null"
```


## Compression statistics for test samples (PostgreSQL)

|Table          |Rows      |Size (PG table)|Size (pg_dump/c)|Size (orc/ZLIB)|Compression ratio (%)|
|---------------|----------|---------------|----------------|---------------|---------------------|
|organization   | 6 300 010|        1407 MB|         290 MB |         275 MB| 19.5 %              |
|person         |14 383 339|        3720 MB|         534 MB |         561 MB| 15 %                | 

```
$ pg_dump -d $dbname --table=organization --file=organization-c.dump --format=c
$ pg_dump -d $dbname --table=person --file=person-с.dump --format=c
```


## Other Utils been used:

From https://github.com/apache/orc 

* orc-contents
* orc-memory
* orc-metadata
* orc-scan
* orc-statistics
* orc-tools

## Test data

GeoLite2 Free Downloadable Databases 
* https://dev.maxmind.com/geoip/geoip2/geolite2/

Postgres Demo Db
* https://postgrespro.ru/education/demodb

MySQL Demo Db
* https://github.com/datacharmer/test_db

MSSQL NorthWind Db
* https://github.com/microsoft/sql-server-samples

OpenStreetMap
* https://planet.openstreetmap.org/

### Native tools:

- orc-contents

```
Usage: orc-contents <filename> [--columns=1,2,...]
Print contents of <filename>.
If columns are specified, only these top-level (logical) columns are printed.
```

- orc-memory
- orc-metadata

```
Usage: orc-metadata [-h] [--help] [-r] [--raw] [-v] [--verbose] <filename>
```

- orc-scan
- orc-statistics

### Apache ORC java tools:

- orc-tools-1.6.0-SNAPSHOT-uber.jar