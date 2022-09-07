#!/bin/bash

set -e

PORT="8001"
MASTERHOST="localhost:45555"
STORAGE="output/mapreduce"
JAVA_PROGRAM="com.jacobr4d.mapreduce.worker.WorkerServer"
JAVA_PROGRAM_ARGS="$PORT $MASTERHOST $STORAGE"

mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
