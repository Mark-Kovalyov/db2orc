#!/bin/bash -v

java org.h2.tools.RunScript -url jdbc:h2:~/test -user sa -script test.zip -options compression zip
