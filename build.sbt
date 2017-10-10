name := "avro-spark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1"

libraryDependencies += "org.springframework" % "spring-beans" % "4.3.11.RELEASE"