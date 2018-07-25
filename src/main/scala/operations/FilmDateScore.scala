package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie
import java.text.SimpleDateFormat
import movierank.operations.Operation

object FilmDateScore extends Operation[RDD[(String,(String, Float))]] {
    def compute(movies: RDD[Movie], context: SparkContext) = {
        val format = new SimpleDateFormat("dd-MM-yyyy")
        val date1 = format.parse("01-01-2003").getTime()/ 1000
        val date2 = format.parse("01-01-2008").getTime()/ 1000

        val pairs = movies.map((mov) =>
                                if (mov.time <= date1)
                                    ((mov.productId, 0), mov.score)
                                else if (mov.time > date2)
                                    ((mov.productId, 2), mov.score)
                                else
                                    ((mov.productId, 1), mov.score)
                            )

            .mapValues((_, 1))

        pairs.reduceByKey{ case ((score1, count1), (score2, count2)) => (score1 + score2, count1 + count2) }
            .map{ case ((key, slot), (score, count)) =>
                                            if (slot == 0)
                                                (key, ("1997-2002",(score / count)))
                                            else if (slot == 1)
                                                (key, ("2003-2007",(score / count)))
                                            else
                                                (key, ("2008-2012",(score / count)))
                                    }
    }
}
