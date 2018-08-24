package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie
import movierank.pageRank
import movierank.userHelpfulness
import movierank.helpfulnessByScore
import movierank.operations.Operation

object PageRank {
    def pageRankAllMovies(movies : RDD[Movie]) = {
        // Helpfulness media degli utenti
        // (userId, helpfulness (tra 0 e 1))
        val helpfulness = userHelpfulness(movies)
                    .filter { case (_,value) => !value.isEmpty }
                    .mapValues { _.get}

        // Helpfulness media delle review per film in base allo score assegnato
        // ((score, productId), helpfulness) per un singolo productId
        val average = helpfulnessByScore(movies)

        val reviews = movies.map( mov => (mov.userId, (mov.score, mov.productId)))
        val reviewHelpfulness = reviews.join(helpfulness).map { case (id, (score, help)) => (score, (id, help)) }

        // Per ogni "gruppo" di review di uno stesso film che assegnano lo stesso score tiro su
        // la helpfulness degli utenti in base alla media del film
        val globalUserHelpfulness = reviewHelpfulness.join(average).map {
            case (score, ((id, help), averageHelpfulness)) =>
                (id, if (help < averageHelpfulness) (help+averageHelpfulness)/2 else help)
        }

        // Se consideriamo piu' di un film alla fine ci sono piu' valori di helpfulness
        // per ogni utente. Si fa la media
        globalUserHelpfulness.aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
            .map { case (userId, help) => (userId, help._1/help._2) }
    }



    def pageRankOneMovie(movies : RDD[Movie], productId : String) = {
        // Helpfulness media degli utenti
        // (userId, helpfulness (tra 0 e 1))
        val helpfulness = userHelpfulness(movies)
                    .filter { case (_,value) => !value.isEmpty }
                    .mapValues { _.get}

        // Helpfulness media delle review per film in base allo score assegnato
        // (score, helpfulness) per un singolo productId
        val average = helpfulnessByScore(movies, productId)

        // Consideriamo solo un film alla volta e la helpfulness delle sue review
        // (score, (userid, help))
        val reviews = movies.filter(_.productId == productId).map( mov => (mov.userId, mov.score))
        val reviewHelpfulness = reviews.join(helpfulness).map { case (id, (score, help)) => (score, (id, help)) }

        // Per ogni "gruppo" di review di uno stesso film che assegnano lo stesso score tiro su
        // la helpfulness degli utenti in base alla media del film
        reviewHelpfulness.join(average).map {
            case (score, ((id, help), averageHelpfulness)) =>
                (id, if (help < averageHelpfulness) (help+averageHelpfulness)/2 else help)
        }
    }

    def compute(movies: RDD[Movie], context: SparkContext) = {
        pageRankAllMovies(movies)
        //pageRankAllMoviesInefficient(movies, context)
    }

    def pageRankAllMoviesInefficient(movies: RDD[Movie], context:SparkContext) = {
        val moviesProductId = movies.map(_.productId).distinct.collect.toList
        var userHelpfulnessRankings = context.emptyRDD[(String, Double)];

        moviesProductId.foreach { id => userHelpfulnessRankings = userHelpfulnessRankings.union(pageRankOneMovie(movies, id))}
        val average = userHelpfulnessRankings
                                    .aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1),
                                                                (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
                                            
        average.map { case (userId, acc) => (userId, acc._1/acc._2) }
    }
}
