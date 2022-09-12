#!/bin/bash
set -e

JAVA_PROGRAM="com.jacobr4d.crawler.utils.RobotsInfo"

URL="$1"
JAVA_PROGRAM_ARGS="$URL"

rm -rf output 
mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
