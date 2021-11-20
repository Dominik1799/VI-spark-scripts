from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
import re



DUMP = "/data/skwiki-pages-articles.xml"
TITLES =  "/data/all_titles_sk.txt"


spark = SparkSession.builder.master("spark://sparkmaster:7077").appName("data preprocessing").getOrCreate()
sc = spark.sparkContext

# read XML, extract title, text and redirect. if not redirect, value is null
initial_df = spark.read.format('xml').options(rowTag='page').load(DUMP)
df = initial_df.selectExpr("title", "revision.text._VALUE as text", "redirect._title as redirect")


# read all titles and replace all _ with spaces
all_titles = spark.read.format("text").load(TITLES)

udf = UserDefinedFunction(lambda x: x.replace("_", " "), StringType())
all_titles = all_titles.select(*[udf(column).alias("value") if column == "value" else column for column in all_titles.columns])

# remove all articles that dont have title in all_titles dataset
df = df.join(all_titles, df.title == all_titles.value).select(df["*"])

wikipedia_namespaces = ["User:", "Wikipedia:", "File:", "MediaWiki:", 
                        "Template:", "Help:", "Category:", "Portal:", 
                        "Draft:", "TimedText:", "Module:", "Gadget:", 
                        "Gadget: definition:", "Special:", "Media:"]


def extract_wikilinks(article):
    all_wikilinks = []
    for wikilink in re.findall("(\[\[.+?\]\])", article):
        wikilink = wikilink.replace("[[", "").replace("]]", "")  # tu bolo lower
        is_namespace = False
        for namespace in wikipedia_namespaces:
            if wikilink.startswith(namespace):
                is_namespace = True
                break
        if is_namespace:
            continue
        # https://en.wikipedia.org/wiki/Help:Wikitext#Wikilinks  <-- cleaning because of this
        if "|" in wikilink:
            wikilink = wikilink.split("|")[0]
        if "#" in wikilink:
            wikilink = wikilink.split("#")[0]
        # NOTE: tu si kontroloval ci je wikilink v all_titles. Ale nejde to. Pri vyhladavani koncovom jednocuho pouzi all_titles set a z neho citaj veci ci tam su alebo nie az.
        if wikilink != "": 
            all_wikilinks.append(wikilink)
    return all_wikilinks


udf = UserDefinedFunction(lambda x: extract_wikilinks(x), ArrayType(StringType()))
df = df.select(*[udf(column).alias("text") if column == "text" else column for column in df.columns])
df = df.selectExpr("title", "text as links", "redirect")


df.write.mode("overwrite").json("/data/output/main")