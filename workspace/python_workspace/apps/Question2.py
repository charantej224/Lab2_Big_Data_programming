from pyspark.sql import *
from pyspark.sql.types import IntegerType

## a. Import the dataset and create df and print Schema
spark = SparkSession \
    .builder \
    .appName("Question2-Part1") \
    .master("local[2]") \
    .getOrCreate()

worldcup_matches = spark.read.csv("DataSet/WorldCupMatches.csv", header=True)

worldcup_matches.printSchema()

## b. Perform  10  intuitive  questions  in  Dataset  (e.g.:  pattern
##    recognition,  topic discussion, most  important  terms,  etc.).
##   Use  your  innovation  to  think  out  of box.

# 1 Renaming columns
worldcup_matches = worldcup_matches.withColumnRenamed('HomeTeam', 'Home_Team')
worldcup_matches = worldcup_matches.withColumnRenamed('HomeGoals', 'Home_Team_Goals')
worldcup_matches = worldcup_matches.withColumnRenamed('AwayGoals', 'Away_Team_Goals')
worldcup_matches = worldcup_matches.withColumnRenamed('AwayTeam', 'Away_Team')

worldcup_matches.printSchema()

# 2 Changing the structType of columns from String to Double
worldcup_matches = worldcup_matches.withColumn('Away_Team_Goals',
                                               worldcup_matches['Away_Team_Goals'].cast(IntegerType()))
worldcup_matches = worldcup_matches.withColumn('Home_Team_Goals',
                                               worldcup_matches['Home_Team_Goals'].cast(IntegerType()))

worldcup_matches.printSchema()

# 3 Matches from 1950
worldcup_matches.filter(worldcup_matches.Year.like("1950")).show()

# 4 Count the total no. of matches played in 1934 && round like preliminary
print("Matches in 1934 : " + str(worldcup_matches.filter(worldcup_matches.Year.like("1934") & worldcup_matches.Round.like("Preliminary round")).count()))

# 5 Count the no.of times Argentina has appeared in semi-finals
worldcup_matches.filter(
    worldcup_matches.Round.like("Semi-finals") & (worldcup_matches.Home_Team.isin('Argentina','Brazil'))).count()

# 6 Print final matches from all world cups.
worldcup_matches.filter(worldcup_matches.Round == 'Final').show()

# 7 pairing stage wise matches with home team
worldcup_matches.crosstab('Round', 'Home_Team').show()

# 8 finding mean, max, min etc of the attendance over the entire matches
worldcup_matches.describe(['Away_Team_Goals']).show()

# 9 Display the max away goals scored year wise
worldcup_matches.groupBy('Year').max('Away_Team_Goals').dropna().show()

# 10 Display the sum of home goals scored, grouped by each of the each team country
worldcup_matches.groupBy('Home_Team').sum('Home_Team_Goals').orderBy('Home_Team').dropna().show(15, truncate=True)

print("END OF QUESTION 2")
