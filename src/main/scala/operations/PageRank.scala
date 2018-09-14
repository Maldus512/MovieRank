package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import movierank.movies.Movie
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
import movierank.{similar, similarWithHelpfulness, save}
import movierank.operations.{UserHelpfulness}

object PageRank {

    def helpfulnessByScore(movies: RDD[Movie], productId:String) = {
        // Coppie valutazione del film - helpfulness della review
        val pairs = movies.filter( mov => mov.productId == productId  && !mov.percentage.isEmpty).map( mov => (mov.score, mov.percentage.get) )

        // Helpfulness media delle review per film in base allo score assegnato
        pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
            .map { case (score, help) => (score, help._1/help._2) }

    }

    def helpfulnessByScore(movies: RDD[Movie]) : RDD[((Double, String), Int)] = {
             // Coppie valutazione del film - helpfulness della review
        val pairs = movies.filter(mov => !mov.percentage.isEmpty).map( mov => ((mov.score, mov.productId), mov.percentage.get) )

        // Helpfulness media delle review per film in base allo score assegnato
        pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
            .map { case (score, help) => (score, help._1/help._2) }
    }


    def pageRankOneMovie(movies : RDD[Movie], productId : String) = {
        // Helpfulness media degli utenti
        // (userId, helpfulness (tra 0 e 1))
        val helpfulness = UserHelpfulness.compute(movies)

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


    def computePageRank_averageInefficient(movies: RDD[Movie], context: SparkContext) = {
        val moviesProductId = movies.map(_.productId).distinct.collect.toList
        var userHelpfulnessRankings = context.emptyRDD[(String, Double)];

        moviesProductId.foreach { id => userHelpfulnessRankings = userHelpfulnessRankings.union(pageRankOneMovie(movies, id))}
        val average = userHelpfulnessRankings
            .aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1),
                                        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

        average.map { case (userId, acc) => (userId, acc._1/acc._2) }
    }


    def computePageRank_average(movies: RDD[Movie]) = {
        // Helpfulness media degli utenti
        // (userId, helpfulness (tra 0 e 1))
        val helpfulness = UserHelpfulness.compute(movies)

        // Helpfulness media delle review per film in base allo score assegnato
        // ((score, productId), helpfulness) per un singolo productId
        val average = helpfulnessByScore(movies)

        val reviews = movies.map( mov => (mov.userId, (mov.score, mov.productId)))
        val reviewHelpfulness = reviews.join(helpfulness)
            .map { case (id, (score_productId, help)) => (score_productId, (id, help)) }

        // Per ogni "gruppo" di review di uno stesso film che assegnano lo stesso score tiro su
        // la helpfulness degli utenti in base alla media del film
        val globalUserHelpfulness = reviewHelpfulness.join(average)
            .map {
                case (score_productId, ((id, help), averageHelpfulness)) =>
                    (id, if (help < averageHelpfulness) (help+averageHelpfulness)/2 else help)
                }

        // Se consideriamo piu' di un film alla fine ci sono piu' valori di helpfulness
        // per ogni utente. Si fa la media
        globalUserHelpfulness.aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
            .map { case (userId, help) => (userId, help._1/help._2) }
    }


    def computePageRank_Naive(movies: RDD[Movie]) = {
        val users_helpfulness = UserHelpfulness.compute(movies)

        val users = movies.map((mov) => (mov.userId, mov))
                        .aggregateByKey(List[(String, Double)]()) ( (x,y) => (y.productId, y.score)::x, _++_)
                        //.aggregateByKey(List[Movie]()) ( (x,y) => y::x, _++_)
                        .join(users_helpfulness)

        val users_graph = users.cartesian(users)
                            .filter({case (u1, u2) => (u1._1 != u2._1)})
                            .map({case (x,y) => similarWithHelpfulness(x,y)});

        //get only similar user with positive edge. if user A is link with user B that has lower helpfulness, this is a negative edge.
        val user_graph_positiveEdge = users_graph.filter((tmp) => tmp.positiveEdge && tmp.similar)

        //la differenza di helpfulness è divisa per 50 perchè l'incremento deve essere lieve ed in relazione alla similitudine (degree)
        val similarUserMap = user_graph_positiveEdge.map((x) => (x.userId1, (x.degree, x.helpfulnessDifference/50, x.helpfulnessId1)))

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        //Il secondo accumulatore è un magheggio per portarmi dietro la helpfulness iniziale
        val result = similarUserMap.aggregateByKey((0.0,0.0)) ((acc, value) => (acc._1+value._2*value._1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2))
                        .map { case (userId, help_acc) => (userId, help_acc._1+help_acc._2) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None
    }


    def computePageRank_noCartesian(movies: RDD[Movie]) = {
        val users_helpfulness = UserHelpfulness.compute(movies)
        users_helpfulness.persist(StorageLevel.MEMORY_AND_DISK_SER)

        val moviesPerUser = movies.map((mov) => (mov.userId, mov))
                        //.aggregateByKey(List[Movie]()) ( (x,y) => y::x, _++_)
                        .aggregateByKey(List[(String, Double)]()) ( (x,y) => (y.productId, y.score)::x, _++_)
                        .join(users_helpfulness)
                        .partitionBy(new HashPartitioner(16))
                        .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val usersPerMovie = movies.map(mov => (mov.productId, mov.userId))
                            .aggregateByKey(List[String]()) ( (x,y) => y::x, _++_)

        val usersToCompare = usersPerMovie.flatMap {
                            case (productId, xs) =>
                                val cartesianProduct = xs.flatMap(x => xs.map(y => (x,y)))
                                                        .filter { case (u1, u2) => (u1 != u2)}
                                cartesianProduct
                        }.distinct()

        val users_graph = usersToCompare.join(moviesPerUser)
                                        .map {
                                            case (userId1, (userId2, joinedContent1)) =>
                                                (userId2, (userId1, joinedContent1))
                                        }
                                        .join(moviesPerUser)
                                        .map {
                                            case (userId2, ((userId1, joinedContent1), joinedContent2)) =>
                                                similarWithHelpfulness((userId1, joinedContent1), (userId2, joinedContent2))
                                        }

        //get only similar user with positive edge. if user A is link with user B that has lower helpfulness, this is a negative edge.
        val user_graph_positiveEdge = users_graph.filter((tmp) => tmp.positiveEdge && tmp.similar)

        //la differenza di helpfulness è divisa per 50 perchè l'incremento deve essere lieve ed in relazione alla similitudine (degree)
        // OPERAZIONE MOLTO COSTOSA v
        val similarUserMap = user_graph_positiveEdge.map((x) => (x.userId1, (x.degree, x.helpfulnessDifference/50, x.helpfulnessId1)))

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        //Il secondo accumulatore è un magheggio per portarmi dietro la helpfulness iniziale (CE ALTRO MODO PER FARLO??)
        val result = similarUserMap.aggregateByKey((0.0,0.0)) ((acc, value) => (acc._1+value._2*value._1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2))
                        .map { case (userId, help_acc) => (userId, help_acc._1+help_acc._2) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None*/
    }

    def computePageRank_Optimized(movies: RDD[Movie]) = {
        val users_helpfulness = UserHelpfulness.compute(movies)
                        .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val users_MovList_helpfulness = movies.map((mov) => (mov.userId, mov))
                        .aggregateByKey(List[(String, Double)]()) ( (x,y) => (y.productId, y.score)::x, _++_)
                        //.aggregateByKey(List[Movie]()) ( (x,y) => y::x, _++_)
                        .join(users_helpfulness)

        val film_user = movies.map((mov) => (mov.userId, mov.productId))
                        //.distinct()
                        .join(users_MovList_helpfulness)
                        .map {
                            case (userId1, (filmId, movList_helpfulness)) =>
                                (filmId, (userId1, movList_helpfulness))
                        }
                        .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val users_graph = film_user.join(film_user)
                        .map {
                            case (filmId, ((userId, movList_helpfulness),((userId2, movList_helpfulness2)))) =>
                                ((userId, movList_helpfulness), (userId2, movList_helpfulness2))
                        }
                        .map {
                            case (x,y) => similarWithHelpfulness(x,y)
                        }

        //get only similar user with positive edge. if user A is link with user B that has lower helpfulness, this is a negative edge.
        val user_graph_positiveEdge = users_graph.filter((tmp) => tmp.positiveEdge && tmp.similar)

        //la differenza di helpfulness è divisa per 50 perchè l'incremento deve essere lieve ed in relazione alla similitudine (degree)
        val similarUserMap = user_graph_positiveEdge.map((x) => ((x.userId1, x.userId2), (x.degree, x.helpfulnessDifference/50, x.helpfulnessId1)))
                            .distinct()
                            .map { case (((x, y), (x_degree, x_helpfulnessDifference, x_helpfulnessId1))) =>
                                    (x, (x_degree, x_helpfulnessDifference, x_helpfulnessId1))
                            }

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        //Il secondo accumulatore è un magheggio per portarmi dietro la helpfulness iniziale (CE ALTRO MODO PER FARLO??)
        val result = similarUserMap.aggregateByKey((0.0,0.0)) ((acc, value) => (acc._1+value._2*value._1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2))
                        .map { case (userId, help_acc) => (userId, help_acc._1+help_acc._2) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None
    }
}
