package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import movierank.movies.Movie

/**
 * Calcola per ogni film lo score medio dato alle sue recensioni
 */
object FilmScore {

    def compute(movies: RDD[Movie]) = {
        val pairs = movies.map((mov) => (mov.productId, mov.score))     // coppie (film, score)
            .mapValues((_, 1))
        // somma gli score dello stesso film, contandone le occorrenze
        pairs.reduceByKey{ case ((score1, count1), (score2, count2)) => (score1 + score2, count1 + count2) }
            .mapValues{ case (score, count) => score / count }  // media
    }
}
