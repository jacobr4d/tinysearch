#!/bin/bash

set -e

PORT="45555"
JAVA_PROGRAM="com.jacobr4d.mapreduce.master.MasterServer"
JAVA_PROGRAM_ARGS="$PORT"

mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
