package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie

object FilmScore {
    def compute(movies: RDD[Movie]) = {
        val pairs = movies.map((mov) => (mov.productId, mov.score))
            .mapValues((_, 1))
        pairs.reduceByKey{ case ((score1, count1), (score2, count2)) => (score1 + score2, count1 + count2) }
            .mapValues{ case (score, count) => score / count }
    }
}
