#!/bin/bash -v

java -cp h2*.jar org.h2.tools.RunScript -url jdbc:h2:/storage/h2/db/db -script create.sql
