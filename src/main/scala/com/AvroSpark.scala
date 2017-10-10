package com

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hailingu on 12/14/15.
  */
object AvroSpark {

  def main(args: Array[String]) {
    val input = "iris.csv"

    val conf = new SparkConf
    conf.set("spark.master", "local[2]")
    conf.set("spark.app.name", "test")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Rcord]))
    val sc = new SparkContext(conf)

    val hconf = new org.apache.hadoop.conf.Configuration
    val job = Job.getInstance
    val schema = ReflectData.get().getSchema(classOf[Rcord])

    val fs = FileSystem.get(hconf)
    if (fs.exists(new Path("avro")))
      fs.delete(new Path("avro"), true)

    AvroJob.setOutputKeySchema(job, schema)
    AvroJob.setInputKeySchema(job, schema)

    sc.textFile(input).map(x => {
     // println(x)
      (new AvroKey(new Rcord(1, x)), NullWritable.get)
    }).saveAsNewAPIHadoopFile("avro",
        classOf[AvroKey[Rcord]],
        classOf[org.apache.hadoop.io.NullWritable],
        classOf[AvroKeyOutputFormat[Rcord]],
        job.getConfiguration)

    val r2 = sc.newAPIHadoopFile[AvroKey[Rcord], NullWritable, AvroKeyInputFormat[Rcord]](
      "avro",
      classOf[AvroKeyInputFormat[Rcord]],
      classOf[AvroKey[Rcord]],
      classOf[NullWritable],
      job.getConfiguration)

    val all = r2.collect()
    for (a <- all) println(a._1.datum().toString)
  }
}
