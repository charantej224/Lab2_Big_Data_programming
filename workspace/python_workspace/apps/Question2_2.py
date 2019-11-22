# Perform any 5 queries in Spark RDD’s and Spark Data Frames.
# Compare the results

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Q2 : spark sql ") \
        .config("spark.some.config.option", "") \
        .getOrCreate()

    spark_context = SparkContext.getOrCreate()

    world_cup_rdd = spark_context.textFile("DataSet/WorldCups.csv", 1)
    world_cup_header = world_cup_rdd.first()
    world_cup_content = world_cup_rdd.filter(lambda line: line != world_cup_header)

    # creating an RDD
    rdd = world_cup_content.map(lambda line: (line.split(","))).collect()
    # print(rdd)
    # no. of Columns
    rdd_len = world_cup_content.map(lambda line: len(line.split(","))).distinct().collect()
    print(rdd_len)
    print("---------- 1. Venues & goals scored ------------")
    # venue - hosted country with highest goals (From RDD)
    rdd1 = (world_cup_content.filter(lambda line: line.split(",")[6] != "NULL")
            .map(lambda line: (line.split(",")[1], int(line.split(",")[6])))
            .takeOrdered(10, lambda x: -x[1]))
    print(rdd1)

    dataframe_schema = StructType([StructField('Year', StringType(), True),
                                   StructField('Country', StringType(), True),
                                   StructField('Winner', StringType(), True),
                                   StructField('Runners-Up', StringType(), True),
                                   StructField('Third', StringType(), True),
                                   StructField('Fourth', StringType(), True),
                                   StructField('GoalsScored', StringType(), True),
                                   StructField('QualifiedTeams', StringType(), True),
                                   StructField('MatchesPlayed', StringType(), True),
                                   StructField('Attendance', StringType(), True)])
    # Create data frame from the RDD
    df = spark.createDataFrame(rdd, dataframe_schema)
    df.show()
    df = df.withColumn('GoalsScored', df['GoalsScored'].cast(IntegerType()))
    df = df.withColumnRenamed('Runners-Up', 'Runnersup')
    # venue - hosted country with highest goals (From DF)
    df.select("Country", "GoalsScored").orderBy("GoalsScored", ascending=False).show(20, truncate=False)
    # venue - hosted country with highest goals (From DF - SQL)
    df.createOrReplaceTempView("df_table")
    spark.sql(" SELECT Country,GoalsScored FROM df_table order by " +
              "GoalsScored Desc Limit 10").show()

    print("---------- 2. Year, venue country = winning country ------------")
    # using RDD
    (world_cup_content.filter(lambda line: line.split(",")[1] == line.split(",")[2])
     .map(lambda line: (line.split(",")[0], line.split(",")[1], line.split(",")[2]))
     .collect())
    # using DF
    df.select("Year", "Country", "Winner").filter(df["Country"] == df["Winner"]).show()
    # using DF - SQL
    spark.sql(" SELECT Year,Country,Winner FROM df_table where Country == Winner order by Year").show()

    print("-------------- 3. Details of years ending in ZERO ---------------")
    # using RDD
    years = ["1930", "1950", "1970", "1990", "2010"]
    (world_cup_content.filter(lambda line: line.split(",")[0] in years)
     .map(lambda line: (line.split(",")[0], line.split(",")[2], line.split(",")[3])).collect())
    # using DF
    df.select("Year", "Winner", "Runnersup").filter(df.Year.isin(years)).show()
    # using DF - SQL

    spark.sql(" SELECT Year,Winner,Runnersup FROM df_table  WHERE " +
              " Year IN ('1930','1950','1970','1990','2010') ").show()

    print("-------------- 4. 2014 world cup stats --------------")
    print("Query using RDD")
    # using RDD
    (world_cup_content.filter(lambda line: line.split(",")[0] == "2014")
     .map(lambda line: (line.split(","))).collect())
    # using DF
    df.filter(df.Year == "2014").show()
    # using DF - Sql
    spark.sql(" Select * from df_table where Year == 2014 ").show()

    print("------------- 5. Max matches played -----------------")
    # Using RDD
    (world_cup_content.filter(lambda line: line.split(",")[8] == "64")
     .map(lambda line: (line.split(","))).collect())
    # using DF
    df = df.withColumn('MatchesPlayed', df['MatchesPlayed'].cast(IntegerType()))
    df.filter(df.MatchesPlayed == 64).show()
    # using DF - SQL
    spark.sql(" Select * from df_table where MatchesPlayed in " +
              "(Select Max(MatchesPlayed) from df_table )").show()
