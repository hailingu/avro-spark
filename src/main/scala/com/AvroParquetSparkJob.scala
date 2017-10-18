package com

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.beans.PropertyAccessorFactory

import scala.reflect.ClassTag

class AvroParquetSparkJob[T: ClassTag] {

  var conf: SparkConf = _
  var sc: SparkContext = _
  var schema: Schema = _
  var schemaStr: String = _
  var job = Job.getInstance()

  def init(conf: SparkConf)(implicit m: ClassTag[T]): AvroParquetSparkJob[T] = {
    this.conf = conf
    this.conf.registerKryoClasses(Array(m.runtimeClass.asInstanceOf[Class[T]],
      classOf[GenericData.Record], classOf[AvroSparkJob[T]], classOf[AvroParquetSparkJob[T]]))
    this.sc = new SparkContext(conf)
    this.schema = ReflectData.get().getSchema(m.runtimeClass.asInstanceOf[Class[T]])
    this.schemaStr = this.schema.toString
    AvroParquetOutputFormat.setSchema(job, schema)
    this
  }

  def saveAsParquetFile(records: RDD[T], path: String, schema: String) = {
    val genericRecordRdd = records.map(record => {
      val schemaIn = new Schema.Parser().parse(schema)
      val genericRecord = new GenericData.Record(schemaIn)
      val fields = schemaIn.getFields.toArray()
      for (f <- fields) {
        val cf = f.asInstanceOf[Schema.Field]
        genericRecord.put(cf.name(), PropertyAccessorFactory.forDirectFieldAccess(record).getPropertyValue(cf.name))
      }
      (null, genericRecord)
    })

    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    job.setOutputValueClass(classOf[GenericData.Record])

    genericRecordRdd.saveAsNewAPIHadoopFile(
      path,
      classOf[Void],
      classOf[GenericData.Record],
      classOf[ParquetOutputFormat[GenericData.Record]],
      job.getConfiguration
    )
  }

  def parquetFile[T](path: String)
                    (implicit m: ClassTag[T]): RDD[T] = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read.format("parquet").load(path).registerTempTable("tmp")
    val rcordEncoder = Encoders.bean(m.runtimeClass.asInstanceOf[Class[T]])
    sqlContext.sql("SELECT * FROM tmp").as(rcordEncoder).rdd
  }
}
