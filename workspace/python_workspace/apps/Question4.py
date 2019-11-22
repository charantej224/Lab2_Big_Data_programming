from graphframes import GraphFrame
from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Q4") \
    .getOrCreate()

group_edges = spark.read.csv("DataSet/group-edges.csv", header=True)
meta_edges = spark.read.csv("DataSet/meta-groups.csv", header=True)

group_edges.createOrReplaceTempView("edges")
meta_edges.createOrReplaceTempView("vertices")

vertices1 = spark.sql("select * from edges")
edges1 = spark.sql("select * from vertices")

vertices = vertices1.withColumnRenamed("group_id", "id").limit(100).distinct()
edges = edges1.withColumnRenamed("group1", "src").limit(500).distinct().withColumnRenamed("group2", "dst").limit(500).distinct()

vertices.show(5)
edges.show(5)

g = GraphFrame(vertices, edges)

g.edges.cache()
g.vertices.cache()
