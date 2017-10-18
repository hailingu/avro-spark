package com

import org.apache.avro.Schema
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

class AvroSparkJob[T: ClassTag] extends java.io.Serializable {

  var sc: SparkContext = _
  var schema: Schema = _

  def init(conf: SparkConf)(implicit  m: ClassTag[T]): AvroSparkJob[T] = {
    conf.registerKryoClasses(Array(m.runtimeClass.asInstanceOf[Class[T]]))
    this.sc = new SparkContext(conf)
    this.schema = ReflectData.get().getSchema(m.runtimeClass.asInstanceOf[Class[T]])
    this
  }

  def saveAvroFile(rdd: RDD[T], path: String): Unit = {
    if (null == this.sc) return
    val job = Job.getInstance()
    AvroJob.setOutputKeySchema(job, schema)
    rdd.map(t => (new AvroKey[T](t), null)).saveAsNewAPIHadoopFile(path,
      classOf[AvroKey[T]],
      classOf[org.apache.hadoop.io.NullWritable],
      classOf[AvroKeyOutputFormat[T]],
      job.getConfiguration)
  }

  def AvroFile(path: String)(implicit m: ClassTag[T]): RDD[T] = {
    if (null == this.sc) return null
    val job = Job.getInstance()
    AvroJob.setInputKeySchema(job, schema)
    sc.newAPIHadoopFile[AvroKey[T], NullWritable, AvroKeyInputFormat[T]](
      "avro",
      classOf[AvroKeyInputFormat[T]],
      classOf[AvroKey[T]],
      classOf[NullWritable],
      job.getConfiguration).map(_._1.datum())
  }
}
