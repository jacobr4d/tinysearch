#!/bin/bash

echo `du -sh output`
echo `wc -l output/mapreduce/hits` "(word url)"
echo `wc -l output/mapreduce/tfs` "(word url tf)"
echo `wc -l output/mapreduce/idfs` "(word idf)"
