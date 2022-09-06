#!/bin/bash
set -e

REPOSITORY="repository"
INDEX="index"
JAVA_PROGRAM="com.jacobr4d.crawler.Crawler"
JAVA_PROGRAM_ARGS="https://en.wikipedia.org/wiki/Tropical_Storm_Carrie_(1972) 1 10 $REPOSITORY $INDEX"

mvn compile
rm -rf "$REPOSITORY" "$INDEX"
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
