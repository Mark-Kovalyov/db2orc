#!/bin/bash -v

java org.h2.tools.Script -url jdbc:h2:~/test -user sa -script test.zip -options compression zip
