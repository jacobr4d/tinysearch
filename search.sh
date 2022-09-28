#!/bin/bash

set -e

JAVA_PROGRAM="com.jacobr4d.searcher.WebInterface"
JAVA_PROGRAM_ARGS=""

mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
