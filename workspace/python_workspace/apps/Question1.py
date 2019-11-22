from pyspark import SparkContext


def map_lines(each_line):
    '''
    each line has only two parameters, so split them and make them a group
    :param each_line:
    :return:
    '''
    each_line = each_line.split(" ")
    profile = each_line[0]
    friends = each_line[1]
    return profile, friends


def reduce(accum, value):
    return list(set(accum) & set(value))


def map_mapped(tuple):
    '''
    after grouping, we need to map them inorder to have keys intersected where values would be mutual friends.
    :param tuple:
    :return:
    '''
    key = tuple[0]
    final = []
    for value in tuple[1]:
        if key < value:
            finalkey = key + "," + value
        else:
            finalkey = value + "," + key
        val = finalkey, list(tuple[1])
        final.append(val)
    return final


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    lines = sc.textFile("DataSet/facebook_combined.txt", 1)
    mapped = lines.map(map_lines).groupByKey()
    grouped = mapped.flatMap(map_mapped)
    final = grouped.reduceByKey(reduce).filter(lambda x: len(x[1]) > 0)
    print(final.collect())
    final.coalesce(1).saveAsTextFile("output/facebook-challenge")
