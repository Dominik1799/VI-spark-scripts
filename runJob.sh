#!/bin/bash

/opt/bitnami/spark/bin/spark-submit --master spark://sparkmaster:7077 /data/scripts/main.py && \
rm /data/output/main/.*.crc  && \
cat /data/output/main/*.json > /data/output/main/merged.jl && \
rm /data/output/main/*.json  && \
echo Success!
