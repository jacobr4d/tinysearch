#!/bin/bash

set -e

JAVA_PROGRAM="com.jacobr4d.indexer.DataUploader"
JAVA_PROGRAM_ARGS=""

rm -rf output/index
mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
