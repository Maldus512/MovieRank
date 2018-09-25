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

/* L'idea generale degli algoritmi qui descritti e' quella di incrementare
    l'utilita' degli utenti che danno voti simili ad altri valutati meglio di loro.
        Esempio:
        Utente A ha dato voto 4/5 al film 1, ricevendo 80 upvote e 20 downvote, con
        una percentuale di 80% helpfulness. Utente B ha dato lo stesso voto, ma
        la sua recensione ha soltanto 10 downvote.
        Parte dell'utilita' media di A va a incrementare quella di B, che ha dato
        la stessa opinione di un utente ritenuto autorevole
         */

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

    /* Algoritmo che modifica il ranking degli utenti secondo la logica
    descritta precedentemente, ma relativamente a un singolo film */
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

    /* Algoritmo che modifica il ranking degli utenti su tutti i film basandosi su quello
    precedente. Dovendo iterare su tutti i film con un ciclo si perdono tutti i vantaggi del
    calcolo distribuito */
    def computePageRank_averageInefficient(movies: RDD[Movie], context: SparkContext) = {
        // Ottieni la lista dei film interamente sul master
        val moviesProductId = movies.map(_.productId).distinct.collect.toList
        var userHelpfulnessRankings = context.emptyRDD[(String, Double)];

        // Per ogni film calcolare il nuovo ranking e costruire un RDD che li contenga tutti
        moviesProductId.foreach { id => userHelpfulnessRankings = userHelpfulnessRankings.union(pageRankOneMovie(movies, id))}
        // Calcolare la media delle utilita' degli utenti cosi' ottenuta
        val average = userHelpfulnessRankings
            .aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1),
                                        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

        average.map { case (userId, acc) => (userId, acc._1/acc._2) }
    }

    /* Versione migliorata dell'algoritmo precedente, che esegue i calcoli su tutti
    i film sin dall'inizio */
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
                        .join(users_helpfulness)

        val users_graph = users.cartesian(users)
                            .filter({case (u1, u2) => (u1._1 != u2._1)})
                            .map({case (x,y) => similarWithHelpfulness(x,y)});

        //get only similar user with positive edge. if user A is link with user B that has lower helpfulness, this is a negative edge.
        val user_graph_positiveEdge = users_graph.filter((tmp) => tmp.positiveEdge && tmp.similar)

        //la differenza di helpfulness è divisa per 50 perchè l'incremento deve essere lieve ed in relazione alla similitudine (degree)
        val similarUserMap = user_graph_positiveEdge.map((x) => (x.userId1, (x.degree, x.helpfulnessDifference, x.helpfulnessId1)))

        //la differenza di helpfulness è divisa per 50 perchè l'incremento deve essere lieve ed in relazione alla similitudine (degree)
        val similarUserMap = user_graph_positiveEdge.map((x) => ((x.userId1, x.userId2), (x.degree, x.helpfulnessDifference, x.helpfulnessId1)))
                            .distinct()
                            .map { case (((x, y), (x_degree, x_helpfulnessDifference, x_helpfulnessId1))) =>
                                    (x, (1-x_degree, x_helpfulnessDifference, x_helpfulnessId1))
                            }

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        // L'accumulatore rappresente: _1: sommatoria(1-degree * helpfulnessDifference); _2: somma utenti collegati; _3: è un magheggio per portarmi dietro la helpfulness iniziale
        val result = similarUserMap.aggregateByKey((0.0, 0, 0.0)) ((acc, value) => (acc._1 + (value._1*value._2), acc._2 + 1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3))
                        .map { case (userId, help_acc) => (userId, (help_acc._1/help_acc._2) + help_acc._3) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None
    }


    /* Prima modifica dell'algoritmo precedente, che utilizza un'operazione di join
        al posto della cartesian
    */
    def computePageRank_noCartesian(movies: RDD[Movie]) = {
        val users_helpfulness = UserHelpfulness.compute(movies)
                            // Siccome la users_helpfulness viene usata piu' volte chiamo persist
                            .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // Per ogni utente la lista di film che ha recensito
        val moviesPerUser = movies.map((mov) => (mov.userId, mov))
                        // La aggregateByKey e' sempre preferibile alla groupByKey
                        .aggregateByKey(List[(String, Double)]()) ( (x,y) => (y.productId, y.score)::x, _++_)
                        .join(users_helpfulness)
                        .partitionBy(new HashPartitioner(64))
                        .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // Per ogni film la lista di utenti che l'hanno recensito
        val usersPerMovie = movies.map(mov => (mov.productId, mov.userId))
                            .aggregateByKey(List[String]()) ( (x,y) => y::x, _++_)

        // Porto avanti un prodotto cartesiano in piccolo tra tutti i film che ogni
        // utente ha recensito
        val usersToCompare = usersPerMovie.flatMap {
                            case (productId, xs) =>
                                val cartesianProduct = xs.flatMap(x => xs.map(y => (x,y)))
                                                        .filter { case (u1, u2) => (u1 != u2)}
                                cartesianProduct
                        }.distinct()

        // faccio l'unione di tutti i mini-prodotti cartesiani
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
        val similarUserMap = user_graph_positiveEdge.map((x) => (x.userId1, (x.degree, x.helpfulnessDifference, x.helpfulnessId1)))

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        // L'accumulatore rappresente: _1: sommatoria(1-degree * helpfulnessDifference); _2: somma utenti collegati; _3: è un magheggio per portarmi dietro la helpfulness iniziale
        val result = similarUserMap.aggregateByKey((0.0, 0, 0.0)) ((acc, value) => (acc._1 + (value._1*value._2), acc._2 + 1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3))
                        .map { case (userId, help_acc) => (userId, (help_acc._1/help_acc._2) + help_acc._3) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None
    }

    def computePageRank_Optimized(movies: RDD[Movie]) = {
        val users_helpfulness = UserHelpfulness.compute(movies)
                        .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val users_MovList_helpfulness = movies.map((mov) => (mov.userId, mov))
                        .aggregateByKey(List[(String, Double)]()) ( (x,y) => (y.productId, y.score)::x, _++_)
                        .join(users_helpfulness)

        val film_user = movies.map((mov) => (mov.userId, mov.productId))
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
        val similarUserMap = user_graph_positiveEdge.map((x) => ((x.userId1, x.userId2), (x.degree, x.helpfulnessDifference, x.helpfulnessId1)))
                            .distinct()
                            .map { case (((x, y), (x_degree, x_helpfulnessDifference, x_helpfulnessId1))) =>
                                    (x, (1-x_degree, x_helpfulnessDifference, x_helpfulnessId1))
                            }

        //l'incremento di helpfulness e' valutato moltiplicanto la diffenza di helpfulness tra gli utenti e moltiplicandola per la similitudine
        // L'accumulatore rappresente: _1: sommatoria(1-degree * helpfulnessDifference); _2: somma utenti collegati; _3: è un magheggio per portarmi dietro la helpfulness iniziale
        val result = similarUserMap.aggregateByKey((0.0, 0, 0.0)) ((acc, value) => (acc._1 + (value._1*value._2), acc._2 + 1, value._3), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3))
                        .map { case (userId, help_acc) => (userId, (help_acc._1/help_acc._2) + help_acc._3) }
                        .rightOuterJoin(users_helpfulness)  //merge user update and user not update

        result.map((x) => if (x._2._1.isEmpty) (x._1,x._2._2) else (x._1,x._2._1.get))  //get value in Some and get 0.0 in None
    }
}
