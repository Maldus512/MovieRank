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

object PageRankLocal {

    def pageRank(movies : RDD[Movie], productId : String) : RDD[(String, Double)] = {
        // Coppie valutazione del film - helpfulness della review
        val pairs = movies.filter( mov => mov.productId == productId  && !mov.percentage.isEmpty).map( mov => (mov.score, mov.percentage.get) )
        
        // Helpfulness media degli utenti
        val helpfulness = userHelpfulness(movies)
                    .filter { case (_,value) => !value.isEmpty }
                    .mapValues { _.get}

        // Helpfulness media delle review per film in base allo score assegnato
        val average = pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
                           .map { case (score, help) => (score, help._1/help._2) }

        // Consideriamo solo un film alla volta
        val reviews = movies.filter(_.productId == productId).map( mov => (mov.userId, mov.score))
        val reviewHelpfulness = reviews.join(helpfulness).map { case (id, (score, help)) => (score, (id, help)) }

        // Per ogni "gruppo" di review di uno stesso film che assegnano lo stesso score tiro su
        // la helpfulness degli utenti in base alla media del film
        reviewHelpfulness.join(average).map {
            case (score, ((id, help), averageHelpfulness)) =>
                (id, if (help < averageHelpfulness) (help+averageHelpfulness)/2 else help)
        }
    }

    def compute(movies: RDD[Movie], context: SparkContext) : RDD[(String, Double)] = {
        val productId = movies.take(30)(29).productId
        pageRank(movies, productId)
    }
}
