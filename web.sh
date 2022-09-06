#!/bin/bash
set -e

REPOSITORY="repository"
JAVA_PROGRAM="com.jacobr4d.crawler.WebInterface"
JAVA_PROGRAM_ARGS="output/repository www"

mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
