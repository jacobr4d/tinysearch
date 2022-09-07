#!/bin/bash

set -e

export REPO_SIZE="50"
curl http://localhost:45555/submitjob -d "jobname=wc&classname=com.jacobr4d.mapreduce.job.InverseDocumentFrequency&input=tfs&output=idfs&map=1&reduce=1"

