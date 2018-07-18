package movierank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movies.Movie

object Main {
    def main(args: Array[String]) = {
        val path: String = args(0)

        //configura Spark
        val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
        val context = new SparkContext(conf)

        val movies = load(path, context)


        movies.take(3).foreach(x => {
            println(x)
        })

        context.stop()
    }
}
