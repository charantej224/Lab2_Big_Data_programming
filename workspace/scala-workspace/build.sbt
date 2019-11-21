name := "scala-workspace"
version := "0.1"
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.2"
libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark1.6-s_2.11"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"