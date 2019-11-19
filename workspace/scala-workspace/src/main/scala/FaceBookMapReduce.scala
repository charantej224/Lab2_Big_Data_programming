import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object FaceBookMapReduce {


  def friendsMapper(line: String) = {
    val words = line.split(" ")
    val key = words(0)
    val pairs = words.slice(1, words.size).map(friend => {
      if (key < friend) ("(" + key + "," + friend + ") -> ") else ("(" + friend + "," + key + ") ->")
    })
    pairs.map(pair => (pair, words.slice(1, words.size).toSet))
  }

  def friendsReducer(accumulator: Set[String], set: Set[String]) = {
    print("reducer")
    var finalSet = Set[String]()
    for (value <- accumulator) {
      if (set.contains(value)) {
        finalSet += value
      }
    }
    finalSet
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("facefriends").setMaster("local[*]");
    val sc = new SparkContext(conf)
    //val file = sc.textFile("DataSet/processed_fb_combined.txt")
    val file = sc.textFile("DataSet/test_file.txt")

    val results = file.flatMap(friendsMapper)
    results.foreach(println)

    val reduced = results.reduceByKey(friendsReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    reduced.collect.foreach(line => {
      println(s"${line._1} ${line._2.mkString(" ")}")
    })

    reduced.coalesce(1).saveAsTextFile("MutualFriends")
  }
}
