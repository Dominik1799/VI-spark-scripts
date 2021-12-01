from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import explode
from pyspark.sql.types import StringType
from pyspark.sql.functions import UserDefinedFunction


def set_redirects(article):
    if article != "link":
        return "redirect"
    else:
        return "link"


DATA = "/data/en_parsed.jl"

spark = SparkSession.builder.master("local[*]").appName("data to graph preprocessing").getOrCreate()
sc = spark.sparkContext

df = spark.read.json(DATA)
edges_df = df.select(df.title, explode(df.links), df.redirect).selectExpr("title as src", "col as dst", "redirect as relationship")
vertices_df = df.selectExpr("title as id", "redirect as relationship")
vertices_df = vertices_df.fillna({"relationship": "link"})
edges_df = edges_df.fillna({"relationship": "link"})
udf = UserDefinedFunction(lambda x: set_redirects(x), StringType())
edges_df = edges_df.select(*[udf(column).alias("relationship") if column == "relationship" else column for column in edges_df.columns])
vertices_df = vertices_df.select(*[udf(column).alias("relationship") if column == "relationship" else column for column in vertices_df.columns])

vertices_df.write.mode("overwrite").json("/data/output/vertices")
edges_df.write.mode("overwrite").json("/data/output/edges")