package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
//import org.apache.spark.graphx._
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie


/**
 * Rappresenta un arco tra due utenti
 *  - similar = true se sono considerati 'simili'
 */
class SimilarityEdge (var userId1 : String, var userId2 : String, var similar : Boolean, var degree : Double) {
    override def toString = {
        s"userId1 : ${this.userId1}, userId2 : ${this.userId2}, similar : ${this.similar}, degree : ${this.degree}"
    }
}

/**
 * Compute some content suggestion on users based on their score and scores given by other people
 */
object UserSuggestion {
    // list of pair (User, [Movies he watched])
    def users(movies : RDD[Movie], context: SparkContext) = {
        movies.map((mov) => (mov.userId, mov))
            .groupByKey()
            //.mapValues((movieList) => context.parallelize(movieList.toList))
    }

    def similar(x: (String, Iterable[Movie]), y: (String, Iterable[Movie])) : SimilarityEdge = {
        val (xId, xs) = x
        val (yId, ys) = y

        val commonMovies = xs.filter((x) =>
            ys.filter((y) => y.productId == x.productId)
            .isEmpty == false
        )
        val differences = commonMovies.map((x) => {
            val score1 = x.score
            val score2 = ys.filter((y) => y.productId == x.productId)
                .head
                .score
            (score1 - score2).abs
        })

        val similarity = if(differences.isEmpty) 5.0
            else (differences.reduce((a,b) => a+b).toFloat / differences.size)

        new SimilarityEdge(xId, yId, similarity <= 1.0, similarity)
    }

    def userGraph(users : RDD[(String, Iterable[Movie])]) = {
        users.cartesian(users)
            .map({case (u1, u2) => this.similar(u1, u2)})
            .filter((edge) => edge.similar)
    }

    
    def compute(movies: RDD[Movie], context: SparkContext) = {
        val users = this.users(movies, context)     : RDD[(String, Iterable[Movie])]
        val userGraph = this.userGraph(users)       : RDD[SimilarityEdge]
        //userGraph.foreach(println)

        val userRelatedMovies = users.cartesian(users)
            .filter({case (u1, u2) =>       // utenti 'simili' (e diversi da sé stesso)
                (u1._1 != u2._1) && this.similar(u1, u2).similar })
            .map({case (u1, u2) =>      // mantengo id di u1 e lista recensioni di u2
                (u1._1, u2._2)
            })
            .flatMapValues(x => x)
            .groupByKey()
        : RDD[(String, Iterable[Movie])]

        //userRelatedMovies.take(10).foreach(println)

        userRelatedMovies.mapValues((relatedMovies : Iterable[Movie]) =>
            relatedMovies.groupBy(_.productId)  // divide in base al film
                .map(group => {
                    // somma degli score per film
                    val sum = group._2.aggregate(0.0)(    // NOTA: 0 è il valore di default
                        (n : Double, mov : Movie) => n + mov.score,       // come aggregare nella partizione
                        (n1 : Double, n2 : Double) => n1 + n2)             // come aggregare tra diverse partiz.
                    val avg = sum / group._2.size     // media per film
                    (group._1, avg)
                })
        ) : RDD[(String, Iterable[(String, Double)])]
    }
}
