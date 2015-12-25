name := "avro-spark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-auth" % "2.6.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.7"

libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.7.7" classifier "hadoop2"

