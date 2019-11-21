import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object Question4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("PAGE RANK")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("PAGE RANK")
      .config(conf = conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val groupEdges = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("DataSet/group-edges.csv")

    val groupVertices = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("DataSet/meta-groups.csv")


    groupVertices.printSchema()
    groupEdges.printSchema()

    val vertices = groupVertices
      .withColumnRenamed("group_id", "id").limit(100)
      .distinct()

    val edges = groupEdges
      .withColumnRenamed("group1", "src").limit(500).distinct()
      .withColumnRenamed("group2", "dst").limit(500).distinct()

    vertices.printSchema()
    edges.printSchema()

    val graphFrame = GraphFrame(vertices, edges)
    graphFrame.cache()

    val pageRankResult = graphFrame.pageRank.resetProbability(0.15).tol(0.01).run()
    pageRankResult.vertices.show()
    pageRankResult.edges.show()

    // Time for some sample spark sql tests.
    groupEdges.createOrReplaceTempView("edges_temp_table")
    groupVertices.createOrReplaceTempView("vertices_temp_table")

    val edgesFromSql = spark.sql("select * from edges_temp_table")
    val verticesFromSql = spark.sql("select * from vertices_temp_table")

    edgesFromSql.show(20)
    verticesFromSql.show(20)

  }

}