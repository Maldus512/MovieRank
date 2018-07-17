//package main.scala.com.maldus.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

class Movie (ms : Map[String, String]) extends Serializable {
    val productId : String= ms getOrElse ("productId", "")
    val userId :String = ms getOrElse ("userId", "")
    val profileName : String = ms getOrElse ("profileName", "")
    val utilita :String = ms getOrElse ("helpfulness", "0")
    val score: Float = ms getOrElse ("score", "0.0") toFloat
    val time : Long = ms getOrElse ("time", "0") toLong
    val summary : String = ms getOrElse ("summary", "")
    val text : String = ms getOrElse("text", "")
    override def toString = "Product: " + productId +
     "\n\t summary: \t" + summary + 
     "\n\t score: \t" + score.toString

}

object ExampleJob {
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