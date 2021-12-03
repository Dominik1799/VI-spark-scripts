from graphframes import *
from pyspark.sql import SparkSession

DATA_LANGUAGE = "en"

VERTICES = "/data/graphs/{lan}_vertices.jl".format(lan=DATA_LANGUAGE)
EDGES = "/data/graphs/{lan}_edges.jl".format(lan=DATA_LANGUAGE)

START = "Slovakia"
END = "Donald Trump"


spark = SparkSession.builder.master("local[*]").appName("pathfinding").getOrCreate()
sc = spark.sparkContext
vertices_df = spark.read.json(VERTICES)
edges_df = spark.read.json(EDGES)
g = GraphFrame(vertices_df, edges_df)
path = g.bfs("id = '{src}'".format(src=START), "id = '{dst}'".format(dst=END)).sample(False, 0.1, seed=0).limit(1)
path.write.mode("overwrite").json("/data/output/path")

