package com

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro._
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.beans.PropertyAccessorFactory

import scala.reflect.ClassTag


object ParquetAvroSpark {
  def main(args: Array[String]): Unit = {
    val input = "src/main/resources/iris.csv"

    val schema = ReflectData.get().getSchema(classOf[Rcord])
    val schemaString = schema.toString

    val conf = new SparkConf
    conf.set("spark.master", "local[2]")
    conf.set("spark.app.name", "test")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Record], classOf[Rcord]))
    val sc = new SparkContext(conf)

    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration)
    if (fs.exists(new Path("avro")))
      fs.delete(new Path("avro"), true)

    val rdd = sc.textFile(input).map(x => {
      val schemaIn = new Schema.Parser().parse(schemaString)
      val record = new GenericData.Record(schemaIn)
      val rcod = new Rcord(1, x)
      val fields = schemaIn.getFields.toArray()
      for (f <- fields) {
        val cf = f.asInstanceOf[Schema.Field]
        record.put(cf.name(), PropertyAccessorFactory.forDirectFieldAccess(rcod).getPropertyValue(cf.name))
      }
      record
    })

    saveAsParquetFile[Record](rdd, "avro", sc, schema.toString)

    val readRdd = parquetFile[Record]("avro", sc)
    val list = readRdd.collect()
    for (l <- list)
      println(l)
  }

  def saveAsParquetFile[T <: IndexedRecord](records: RDD[T], path: String, spark: SparkContext, schemaString: String)(implicit m: ClassTag[T]) = {
    val keyedRecords: RDD[(Void, T)] = records.map(record => (null, record))
    spark.hadoopConfiguration.setBoolean("parquet.enable.summary-metadata", false)
    val job = Job.getInstance(spark.hadoopConfiguration)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport[T]])
    val schemaIn = new Schema.Parser().parse(schemaString)
    AvroParquetOutputFormat.setSchema(job, schemaIn)
    keyedRecords.saveAsNewAPIHadoopFile(
      path,
      classOf[Void],
      m.runtimeClass.asInstanceOf[Class[T]],
      classOf[ParquetOutputFormat[T]],
      job.getConfiguration
    )
  }

  def parquetFile[T](path: String, sc: SparkContext)(implicit m: ClassTag[T]): RDD[T] = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])
    AvroParquetInputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
    val file = sc.newAPIHadoopFile(
      path,
      classOf[AvroParquetInputFormat[T]],
      classOf[Void],
      m.runtimeClass.asInstanceOf[Class[T]],
      job.getConfiguration)
    file.map({ case (void, record) => record })
  }
}


