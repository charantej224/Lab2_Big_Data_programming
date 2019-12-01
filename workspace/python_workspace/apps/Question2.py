from pyspark.sql import *
from pyspark.sql.types import IntegerType

'''
a. Import the dataset and create df and print Schema
'''
spark_session = SparkSession \
    .builder \
    .appName("Question2-Part1") \
    .master("local[2]") \
    .getOrCreate()

worldcup_matches = spark_session.read.csv("DataSet/WorldCupMatches.csv", header=True)

worldcup_matches.printSchema()

'''
b. Perform  10  intuitive  questions  in  Dataset  (e.g.:  pattern recognition,  
    topic discussion, most  important  terms,  etc.). Use  your  innovation  to  think  out  of box.
'''
# 1 Renaming columns
worldcup_matches = worldcup_matches.withColumnRenamed('HomeTeam', 'Home_Team')
worldcup_matches = worldcup_matches.withColumnRenamed('HomeGoals', 'Home_Team_Goals')
worldcup_matches = worldcup_matches.withColumnRenamed('AwayGoals', 'Away_Team_Goals')
worldcup_matches = worldcup_matches.withColumnRenamed('AwayTeam', 'Away_Team')

print("1. Renaming Columns")
worldcup_matches.printSchema()

# 2 Changing the structType of columns from String to Integer

worldcup_matches = worldcup_matches.withColumn('Away_Team_Goals',
                                               worldcup_matches['Away_Team_Goals'].cast(IntegerType()))
worldcup_matches = worldcup_matches.withColumn('Home_Team_Goals',
                                               worldcup_matches['Home_Team_Goals'].cast(IntegerType()))

print("2. Changing the structType of columns from String to Integer")
worldcup_matches.printSchema()

# 3 Print final matches from all world cups.
print("3 Print final matches from all world cups.")
worldcup_matches.filter(worldcup_matches.Round == 'Final').show()

# 4 Count the no.of times Argentina has appeared in semi-finals
print("4 Count the no.of times Argentina and Brazil has appeared in semi-finals")
print(worldcup_matches.filter(
    worldcup_matches.Round.like("Semi-finals") & (worldcup_matches.Home_Team.isin('Argentina', 'Brazil'))).count())

# 5 Matches from 1950
print("5 Matches from 1950")
worldcup_matches.filter(worldcup_matches.Year.like("1950")).show()

# 6 Display the sum of home goals scored, grouped by each of the each team country
print("6 Display the sum of home goals scored, grouped by each of the each team country")
worldcup_matches.groupBy('Home_Team').sum('Home_Team_Goals').orderBy('Home_Team').dropna().show(15, truncate=True)

# 7 maximum number of goals scored year wise.
print("7 maximum number of goals scored year wise.")
worldcup_matches.groupBy('Year').max('Away_Team_Goals').dropna().show()

# 8 describing the world cup matches.
print("8 describing the world cup matches.")
worldcup_matches.describe(['Away_Team_Goals']).show()

# 9 cross tab with home team
print("9 cross tab with home team")
worldcup_matches.crosstab('Round', 'Home_Team').show()

# 10 bring out the count for total number of matches in 1934 && round like preliminary
print("10 bring out the count for total number of matches in 1934 && round like preliminary")
print("Matches in 1934 : " + str(worldcup_matches.filter(
    worldcup_matches.Year.like("1934") & worldcup_matches.Round.like("Preliminary round")).count()))

print("END OF QUESTION 2")
