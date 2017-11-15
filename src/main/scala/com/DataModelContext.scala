package com

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext}
import org.springframework.beans.PropertyAccessorFactory

import scala.reflect.ClassTag

class DataModelContext(val sc: SparkContext) {

  def saveAvroFile[T](rdd: RDD[T], path: String)(implicit m: ClassTag[T]): Unit = {
    if (null == this.sc) return
    val job = Job.getInstance()
    val schema = ReflectData.get().getSchema(m.runtimeClass.asInstanceOf[Class[T]])
    AvroJob.setOutputKeySchema(job, schema)
    rdd.map(t => (new AvroKey[T](t), null)).saveAsNewAPIHadoopFile(path,
      classOf[AvroKey[T]],
      classOf[org.apache.hadoop.io.NullWritable],
      classOf[AvroKeyOutputFormat[T]],
      job.getConfiguration)
  }

  def AvroFile[T](path: String)(implicit m: ClassTag[T]): RDD[T] = {
    if (null == this.sc) return null
    val job = Job.getInstance()
    val schema = ReflectData.get().getSchema(m.runtimeClass.asInstanceOf[Class[T]])
    AvroJob.setInputKeySchema(job, schema)
    sc.newAPIHadoopFile[AvroKey[T], NullWritable, AvroKeyInputFormat[T]](
      "avro",
      classOf[AvroKeyInputFormat[T]],
      classOf[AvroKey[T]],
      classOf[NullWritable],
      job.getConfiguration).map(_._1.datum())
  }

  def saveAsParquetFile[T](records: RDD[T], path: String)(implicit m: ClassTag[T]) = {
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

  def parquetFile[T](path: String)
                    (implicit m: ClassTag[T]): RDD[T] = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read.format("parquet").load(path).registerTempTable("tmp")
    val rcordEncoder = Encoders.bean(m.runtimeClass.asInstanceOf[Class[T]])
    sqlContext.sql("SELECT * FROM tmp").as(rcordEncoder).rdd
  }
}
