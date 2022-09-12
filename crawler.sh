#!/bin/bash
set -e

JAVA_PROGRAM="com.jacobr4d.crawler.Crawler"

MAXSIZEMB="1"
SEED="input/seed"
REPOSITORY="output/repository"
INDEX="output/index"
MAPREDUCEOUT="output/mapreduce/hits"
JAVA_PROGRAM_ARGS="$MAXSIZEMB $SEED $REPOSITORY $INDEX $MAPREDUCEOUT"

rm -rf output 
mvn compile
mvn exec:java -Dexec.mainClass="$JAVA_PROGRAM" -Dexec.args="$JAVA_PROGRAM_ARGS"
