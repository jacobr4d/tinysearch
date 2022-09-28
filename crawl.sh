#!/bin/bash
set -e

JAVA_PROGRAM="com.jacobr4d.crawler.Crawler"

MAXSIZEMB="1"
SEED="input/seed"
HITSPATH="output/hits"
LINKSPATH="output/links"
JAVA_PROGRAM_ARGS="$MAXSIZEMB $SEED $HITSPATH $LINKSPATH"

rm -rf output 
mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
