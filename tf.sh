#!/bin/bash

set -e

curl http://localhost:45555/submitjob -d "jobname=wc&classname=com.jacobr4d.mapreduce.job.TermFrequency&input=hits&output=tfs&map=1&reduce=1"

