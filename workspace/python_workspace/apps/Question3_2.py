import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def map_json(each_tweet):
    json_dict_tweet = json.loads(each_tweet.encode('utf-8'))
    return json_dict_tweet["text"].split(" ")


spark_context = SparkContext("local[2]", "Twitter Demo").setLogLevel('ERROR')
# 10 seconds waiting. doesn't make it real time streaming
streaming_context = StreamingContext(spark_context, 10)
lines = streaming_context.socketTextStream('localhost', 5656)

word_count = lines.flatMap(lambda line: map_json(line)).map(lambda word: (word, 1)).pairs.reduceByKey(
    lambda x, y: x + y)
print(word_count.collect())
streaming_context.start()
streaming_context.awaitTermination()
