from graphframes import GraphFrame
from pyspark import *
from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Q2 : spark sql ") \
    .config("spark.some.config.option", "") \
    .getOrCreate()

group_edges = spark.read.csv("/home/charan/workspaces/big_data_programming/Lab2_Big_Data_programming/workspace/python_workspace/apps/DataSet/group-edges.csv", header=True)
meta_edges = spark.read.csv("/home/charan/workspaces/big_data_programming/Lab2_Big_Data_programming/workspace/python_workspace/apps/DataSet/meta-groups.csv", header=True)

group_edges.createOrReplaceTempView("e")
meta_edges.createOrReplaceTempView("g")

g1 = spark.sql("select * from g")
e1 = spark.sql("select * from e")

vertices = g1.withColumnRenamed("group_id", "id").limit(100).distinct()
edges = e1.withColumnRenamed("group1", "src").limit(500).distinct().withColumnRenamed("group2", "dst").limit(500).distinct()

vertices.show(5)
edges.show(5)

g = GraphFrame(vertices, edges)

g.edges.cache()
g.vertices.cache()
