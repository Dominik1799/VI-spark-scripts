#!/bin/bash

/opt/bitnami/spark/bin/spark-submit --master local[*] --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 /data/scripts/parseToGraph.py && \
echo Cleaning1 && \
rm /data/output/vertices/.*.crc  && \
echo Concating && \
cat /data/output/vertices/*.json > /data/output/vertices/vertices.jl && \
echo Cleaning2 && \
rm /data/output/vertices/*.json  && \
echo Success!  && \
echo Cleaning3 && \
rm /data/output/edges/.*.crc  && \
echo Concating && \
cat /data/output/edges/*.json > /data/output/edges/edges.jl && \
echo Cleaning4 && \
rm /data/output/edges/*.json  && \
echo Success!

