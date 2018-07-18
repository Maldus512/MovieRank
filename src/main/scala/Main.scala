package main.scala.movierank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movies.Movie


object Main {
  def main(args: Array[String]) {
    val path: String = args(0)
    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
	val context = new SparkContext(conf)

    val hconf = new org.apache.hadoop.mapreduce.Job().getConfiguration
    hconf.set("textinputformat.record.delimiter", "\n\n")

    val usgRDD = context.newAPIHadoopFile(
        path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hconf)
    .map({ case (_, v) => v.toString })

    val blocks: RDD[Seq[String]] = usgRDD.map(_.split("\n"))
    val movies: RDD[Movie] = blocks.map((xs :Seq[String]) => {
        val values = Map[String, String]()
        val res = xs.foldLeft(values)((ms: Map[String,String], element:String) => {
            val k = element.split(':')(0).split('/')(1)
            val v = element.split(':')(1).trim
            ms.updated(k,v)
        })
        new Movie(res)
    })

    movies.take(3).foreach(x => {
        println(x)
    })

    context.stop()
  }
}