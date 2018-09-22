package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie
/*
    Banale calcolo della helpfulness media per ogni utente.
 */
object UserHelpfulness {
    def compute(movies: RDD[Movie]) : RDD[(String, Double)] = {
        val pairs = movies.map((mov) => (mov.userId, (mov.percentage, 1)))
            .mapValues{ case (helpfulness, accumulator) => (if (helpfulness.isEmpty) (0, accumulator) else (helpfulness.get, accumulator) )}
            .reduceByKey{ case ((help_1, acc_1), (help_2, acc_2)) => (help_1 + help_2, acc_1 + acc_2) }
        pairs.mapValues{ case (help, num_review) => (help/num_review)}
    }
}
