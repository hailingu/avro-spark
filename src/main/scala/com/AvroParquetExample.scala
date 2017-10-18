package com

import com.model.DataModel
import org.apache.spark.SparkConf

object AvroParquetExample {

  def main(args: Array[String]): Unit = {
    val input = "src/main/resources/iris.csv"
    val output = "avro"

    val conf = new SparkConf
    conf.set("spark.master", "local[1]")
    conf.set("spark.app.name", "test")
    val avroParquetSparkJob = new AvroParquetSparkJob[DataModel].init(conf)

    val dataModelRdd = avroParquetSparkJob.sc.textFile(input).map(record => {
      val dataModel = new DataModel()
      dataModel.i = 1
      dataModel.s = record
      dataModel
    })

    //avroParquetSparkJob.saveAsParquetFile(dataModelRdd, output, avroParquetSparkJob.schemaStr)
    avroParquetSparkJob.saveAsParquetFile(dataModelRdd, output)
    val retrieveRdd = avroParquetSparkJob.parquetFile[DataModel](output)
    retrieveRdd.collect().foreach(println(_))
  }

}
