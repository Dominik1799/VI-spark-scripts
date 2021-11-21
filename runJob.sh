#!/bin/bash

/opt/bitnami/spark/bin/spark-submit --master spark://sparkmaster:7077 /data/scripts/main.py && \
echo Cleaning1 && \
rm /data/output/main/.*.crc  && \
echo Concating && \
cat /data/output/main/*.json > /data/output/main/merged.jl && \
echo Cleaning2 && \
rm /data/output/main/*.json  && \
echo Success!
