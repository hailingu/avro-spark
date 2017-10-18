package com

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.beans.PropertyAccessorFactory

import scala.reflect.ClassTag

class AvroParquetSparkJob[T: ClassTag] extends java.io.Serializable {

  var sc: SparkContext = _

  def init(conf: SparkConf)(implicit m: ClassTag[T]): AvroParquetSparkJob[T] = {
    conf.registerKryoClasses(Array(m.runtimeClass.asInstanceOf[Class[T]],
      classOf[GenericData.Record], classOf[AvroParquetSparkJob[T]]))
    this.sc = new SparkContext(conf)
    this
  }

  def saveAsParquetFile(records: RDD[T], path: String)(implicit m: ClassTag[T]) = {
    val schema = ReflectData.get().getSchema(m.runtimeClass.asInstanceOf[Class[T]])
    val job = Job.getInstance()
    AvroParquetOutputFormat.setSchema(job, schema)
    val schemaString = schema.toString
    val genericRecordRdd = records.map(record => {
      val schema = new Schema.Parser().parse(schemaString)
      val genericRecord = new GenericData.Record(schema)
      val fields = schema.getFields.toArray()
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


  def parquetFile(path: String)
                    (implicit m: ClassTag[T]): RDD[T] = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read.format("parquet").load(path).registerTempTable("tmp")
    val rcordEncoder = Encoders.bean(m.runtimeClass.asInstanceOf[Class[U]])
    sqlContext.sql("SELECT * FROM tmp").as(rcordEncoder).rdd
  }
}
