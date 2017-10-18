name := "avro-spark"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += "projectlombok.org" at "http://projectlombok.org/mavenrepo"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll ExclusionRule(organization = "javax.servlet")

libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.7.0"

libraryDependencies += "org.springframework" % "spring-beans" % "4.3.11.RELEASE"

libraryDependencies += "org.projectlombok" % "lombok" % "1.14.4"