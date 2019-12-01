

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

'''
 Perform any 5 queries in Spark RDD’s and Spark Data Frames.
 Compare the results
'''

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Question2-Part2") \
        .master("local[2]") \
        .getOrCreate()

    spark_context = SparkContext.getOrCreate()

    world_cup_rdd = spark_context.textFile("DataSet/WorldCups.csv", 1)
    world_cup_header = world_cup_rdd.first()
    world_cup_content = world_cup_rdd.filter(lambda line: line != world_cup_header)

    rdd_for_data_frame = world_cup_content.map(lambda line: (line.split(","))).collect()
    # print(rdd)
    # no. of Columns
    rdd_len = world_cup_content.map(lambda line: len(line.split(","))).distinct().collect()
    print(rdd_len)

    # RDD - hosting country
    rdd1 = (world_cup_content.filter(lambda line: line.split(",")[6] != "NULL")
            .map(lambda line: (line.split(",")[1], int(line.split(",")[6])))
            .takeOrdered(10, lambda x: -x[1]))
    print(rdd1)

    data_frame_schema = StructType([StructField('Year', StringType(), True),
                                    StructField('Country', StringType(), True),
                                    StructField('Winner', StringType(), True),
                                    StructField('Runners-Up', StringType(), True),
                                    StructField('Third', StringType(), True),
                                    StructField('Fourth', StringType(), True),
                                    StructField('GoalsScored', StringType(), True),
                                    StructField('QualifiedTeams', StringType(), True),
                                    StructField('MatchesPlayed', StringType(), True),
                                    StructField('Attendance', StringType(), True)])

    # creating dataframe from RDD
    world_cup_data_frame = spark.createDataFrame(rdd_for_data_frame, data_frame_schema)
    world_cup_data_frame.show()
    world_cup_data_frame = world_cup_data_frame.withColumn('GoalsScored',
                                                           world_cup_data_frame['GoalsScored'].cast(IntegerType()))
    world_cup_data_frame = world_cup_data_frame.withColumnRenamed('Runners-Up', 'Runnersup')

    # Registering Temp table.
    world_cup_data_frame.createOrReplaceTempView("worldcup_table")

    print("1. countries with highest number of goals")
    # 1. countries with highest number of goals (From DF)
    world_cup_data_frame.select("Country", "GoalsScored").orderBy("GoalsScored", ascending=False).show(10,
                                                                                                       truncate=False)
    # hosted country with highest goals (From DF - SQL)
    spark.sql(" select Country,GoalsScored FROM worldcup_table order by " +
              "GoalsScored Desc Limit 10").show()

    print("2. Max matches played those are greater than 22")
    # Using RDD
    (world_cup_content.filter(lambda line: int(line.split(",")[8]) > 22)
     .map(lambda line: (line.split(","))).collect())
    # using DF
    matches_played = world_cup_data_frame.withColumn('MatchesPlayed',
                                                           world_cup_data_frame['MatchesPlayed'].cast(IntegerType()))
    matches_played.filter(matches_played.MatchesPlayed > 22).show()

    # DataFrame - SQL
    spark.sql(" select * from worldcup_table where MatchesPlayed > 22").show()

    print("3. getting the details of the year 2010..")
    # using RDD
    (world_cup_content.filter(lambda line: line.split(",")[0] == "2010")
     .map(lambda line: (line.split(","))).collect())
    # using DF
    world_cup_data_frame.filter(world_cup_data_frame.Year == "2010").show()
    # DataFrame - SQL
    spark.sql(" select * from worldcup_table where Year == 2010 ").show()

    print("4. Year, hosting game country != that year winning country")
    # using RDD
    (world_cup_content.filter(lambda line: line.split(",")[1] != line.split(",")[2])
     .map(lambda line: (line.split(",")[0], line.split(",")[1], line.split(",")[2]))
     .collect())
    # using DF
    world_cup_data_frame.select("Year", "Country", "Winner").filter(
        world_cup_data_frame["Country"] != world_cup_data_frame["Winner"]).show()
    # DataFrame - SQL
    spark.sql(" select Year,Country,Winner FROM worldcup_table where Country != Winner order by Year").show()

    print("5. check for some random years")
    # using RDD
    years = ["1930", "1950", "1966", "1974", "1978"]
    (world_cup_content.filter(lambda line: line.split(",")[0] in years)
     .map(lambda line: (line.split(",")[0], line.split(",")[2], line.split(",")[3])).collect())

    # using DF
    world_cup_data_frame.select("Year", "Winner", "Runnersup").filter(world_cup_data_frame.Year.isin(years)).show()

    # DataFrame - SQL
    spark.sql(" select Year,Winner,Runnersup FROM worldcup_table  WHERE " +
              " Year IN ('1930','1950','1970','1990','2010') ").show()
