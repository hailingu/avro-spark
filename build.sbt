name := "avro-spark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll ExclusionRule(name = "servlet")