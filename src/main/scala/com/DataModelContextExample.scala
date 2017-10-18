package com

import com.model.DataModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object DataModelContextExample {


  def main(args: Array[String]): Unit = {
    val input = "src/main/resources/iris.csv"
    val output = "avro"

    val conf = new SparkConf
    conf.set("spark.master", "local[1]")
    conf.set("spark.app.name", "test")
    conf.registerKryoClasses(Array(classOf[DataModel]))

    val sc = new SparkContext(conf)
    val dataModelContext = new DataModelContext(sc)

    val dataModelRdd = dataModelContext.sc.textFile(input).map(record => {
      val dataModel = new DataModel()
      dataModel.i = 1
      dataModel.s = record
      dataModel
    })

    dataModelContext.saveAvroFile[DataModel](dataModelRdd, output)
    val retrieveRdd = dataModelContext.AvroFile[DataModel](output)
    retrieveRdd.collect().foreach(println(_))
  }
}
