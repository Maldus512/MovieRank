package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie

object LengthHelpfulness {
    def compute(movies: RDD[Movie]) = {
        val Step =512;

        val pairs = movies.filter(_.percentage != None).map(mov => (mov.text.size/Step, mov.percentage.getOrElse(0)))
        val steps = pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
        val average = steps.map { case (step, value) => (step*512, value._1/value._2) }
        average.sortByKey().map{ case (step, value) => ( "< " + step.toString, value) }
    }
}
