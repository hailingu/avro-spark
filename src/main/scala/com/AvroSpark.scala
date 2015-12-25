package com

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hailingu on 12/14/15.
 */
object AvroSpark {

  def main(args: Array[String]) {
    val input = "/Users/hailingu/Downloads/spark-1.2.0.2.2.0.0-82-bin-2.6.0.2.2.0.0-2041/LICENSE"

    val conf = new SparkConf
    val sc = new SparkContext(conf)



    System.out.println("acb")

    val hconf = new org.apache.hadoop.conf.Configuration
    val job = Job.getInstance
    val schema = ReflectData.get().getSchema(classOf[Rcord])

    AvroJob.setOutputKeySchema(job, schema)

    sc.textFile(input).map(x=>(new AvroKey(new Rcord(1, x)), NullWritable.get)).saveAsNewAPIHadoopFile("local",
      classOf[Rcord],
      classOf[org.apache.hadoop.io.NullWritable],
      classOf[AvroKeyOutputFormat[Rcord]],
      job.getConfiguration)
  }
}
