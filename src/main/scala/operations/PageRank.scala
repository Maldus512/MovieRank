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

class SimilarityHelpfulnessEdge (var userId1 : String, var helpfulnessId1 : Double, var userId2 : String, var similar : Boolean, var degree : Double, var positiveEdge : Boolean, var helpfulnessDifference : Double) {
    override def toString = {
        s"userId1 : ${this.userId1}, userId2 : ${this.userId2}, similar : ${this.similar}, degree : ${this.degree}"
    }
}

object PageRank {

    /* Considera due coppie (userId, (insieme di film visti dall'utente, utilita' media dell'utente)) */
    def similar(x: (String, (Iterable[Movie], Double)), y: (String, (Iterable[Movie], Double))) : SimilarityHelpfulnessEdge = {
        val (xId, (xs, helpfulness_X)) = x
        val (yId, (ys, helpfulness_Y)) = y

        /* Intersezioni dei film visti da xId e yId */
        val commonMovies = xs.filter((x) =>
            ys.filter((y) => y.productId == x.productId)
                .isEmpty == false
        )


        val differences = commonMovies.map((x) => {
            val score1 = x.score
            val score2 = ys.filter((y) => y.productId == x.productId).head.score
            (score1 - score2).abs
        })

        val similarity = if(differences.isEmpty) 5.0
            else (differences.reduce((a,b) => a+b).toFloat / differences.size)

        val helpfulness_difference = (helpfulness_Y - helpfulness_X)
        new SimilarityHelpfulnessEdge(xId, helpfulness_X, yId, similarity <= 0.0, similarity, helpfulness_difference > 0, helpfulness_difference)
    }


    def global_pageRank(movies : RDD[Movie]) = {

        val users_helpfulness = userHelpfulness(movies)

        val users = movies.map((mov) => (mov.userId, mov))
                        .groupByKey()
                        .join(users_helpfulness)

        val users_graph = users.cartesian(users)
                            .filter({case (u1, u2) => (u1._1 != u2._1)})
                            .map({case (x,y) => this.similar(x,y)});

        //get only similar user with positive edge. if user A is link with user B that has lower helpfulness, this is a negative edge.
        val user_graph_positiveEdge = users_graph.filter((tmp) => tmp.positiveEdge && tmp.similar)

        //la differenza di helpfulness è divisa per 50 perchè l'incremento deve essere lieve ed in relazione alla similitudine (degree)
        val similarUserMap = user_graph_positiveEdge.map((x) => (x.userId1, (x.degree, x.helpfulnessDifference/50, x.helpfulnessId1)))

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        //Il secondo accumulatore è un magheggio per portarmi dietro la helpfulness iniziale (CE ALTRO MODO PER FARLO??)
        val result = similarUserMap.aggregateByKey((0.0,0.0)) ((acc, value) => (acc._1+value._2*value._1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2))
                        .map { case (userId, help_acc) => (userId, help_acc._1+help_acc._2) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None
    }


    def pageRankAllMovies(movies : RDD[Movie]) = {
        // Helpfulness media degli utenti
        // (userId, helpfulness (tra 0 e 1))
        val helpfulness = userHelpfulness(movies)

        // Helpfulness media delle review per film in base allo score assegnato
        // ((score, productId), helpfulness) per un singolo productId
        val average = helpfulnessByScore(movies)

        val reviews = movies.map( mov => (mov.userId, (mov.score, mov.productId)))
        val reviewHelpfulness = reviews.join(helpfulness).map { case (id, (score_productId, help)) => (score_productId, (id, help)) }

        // Per ogni "gruppo" di review di uno stesso film che assegnano lo stesso score tiro su
        // la helpfulness degli utenti in base alla media del film
        val globalUserHelpfulness = reviewHelpfulness.join(average).map {
            case (score_productId, ((id, help), averageHelpfulness)) =>
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

    def pageRankAllMoviesInefficient(movies: RDD[Movie], context:SparkContext) = {
        val moviesProductId = movies.map(_.productId).distinct.collect.toList
        var userHelpfulnessRankings = context.emptyRDD[(String, Double)];

        moviesProductId.foreach { id => userHelpfulnessRankings = userHelpfulnessRankings.union(pageRankOneMovie(movies, id))}
        val average = userHelpfulnessRankings
                                    .aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1),
                                                                (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
                                            
        average.map { case (userId, acc) => (userId, acc._1/acc._2) }
    }

    def compute(movies: RDD[Movie], context: SparkContext) = {
        val moviesProductId = movies.map(_.productId).distinct
        val productId = movies.take(1)(0).productId
        //println(productId)
        //pageRankOneMovie(movies, productId)
        pageRankAllMovies(movies);
        //global_pageRank(movies);
    }
}
