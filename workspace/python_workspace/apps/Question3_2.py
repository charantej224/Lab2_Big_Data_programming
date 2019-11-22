import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def map_words(input_value):
    str_value = input_value
    dict_json = json.loads(str_value)
    return dict_json['text'].split(" ")


if __name__ == "__main__":
    sc = SparkContext(appName="SparkStreaming")
    ssc = StreamingContext(sc, 10)
    lines = ssc.socketTextStream("localhost", 5656)
    word_counts = lines.flatMap(map_words).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    word_counts.pprint()
    ssc.start()
    ssc.awaitTermination()
