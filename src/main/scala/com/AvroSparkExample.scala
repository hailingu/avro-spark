package com

import com.model.DataModel
import org.apache.spark.SparkConf

object AvroSparkExample {

  def main(args: Array[String]): Unit = {
    val input = "src/main/resources/iris.csv"
    val output = "avro"

    val conf = new SparkConf
    conf.set("spark.master", "local[1]")
    conf.set("spark.app.name", "test")
    val avroSparkJob = new AvroSparkJob[DataModel].init(conf)

    val dataModelRdd = avroSparkJob.sc.textFile(input).map(record => {
      val dataModel = new DataModel()
      dataModel.i = 1
      dataModel.s = record
      dataModel
    })

    avroSparkJob.saveAvroFile(dataModelRdd, output)
    val retrieveRdd = avroSparkJob.AvroFile[DataModel](output)
    retrieveRdd.collect().foreach(println(_))
  }
}
