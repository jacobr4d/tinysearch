#!/bin/bash

set -e

JAVA_PROGRAM="com.jacobr4d.mapreduce.Launcher"
JAVA_PROGRAM_ARGS="45555 8001 localhost:45555 output/mapreduce"

mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"

