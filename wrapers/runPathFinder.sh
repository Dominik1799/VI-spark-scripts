#!/bin/bash

/opt/bitnami/spark/bin/spark-submit --master local[*] --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 /data/scripts/findPath.py  && \
echo Cleaning1 && \
rm /data/output/path/.*.crc  && \
echo Concating && \
cat /data/output/path/*.json > /data/output/path/result.json && \
python /data/scripts/printPath.py /data/output/path/result.json
