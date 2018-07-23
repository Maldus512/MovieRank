package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie

object ClientScore {
    def compute(movies: RDD[Movie], context: SparkContext) = {
        val pairs = movies.map((mov) => (mov.userId, mov.helpfulness))
            .mapValues((helpfulness) => (helpfulness.split("/")(0), helpfulness.split("/")(1)))
        pairs.reduceByKey{ case ((score_pos1, score_tot1), (score_pos2, score_tot2)) => (score_pos1 + score_pos2, score_tot1 + score_tot2) }
            .mapValues{ case (score_pos, score_tot) => (if (score_tot.toInt == 0) None else (score_pos.toDouble / score_tot.toDouble)*100) }
    }
}
