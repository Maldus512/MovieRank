package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.Map
import movierank.movies.Movie
import movierank.{similar}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner


/**
 * Cerca di prevedere il gradimento di certi film per ogni utente, basandosi sugli score che ha assegnato
 *  e su quelli assegnati da altri utenti con gusti 'simili'
 */
object UserSuggestion {
    // lista di coppie (User, [Film visti])
    def users(movies : RDD[Movie]) = {
        movies.map((mov) => (mov.userId, mov))
            .aggregateByKey(List[(String, Double)]()) ( (x,y) => (y.productId, y.score)::x, _++_)
    }

    // prima versione dell'algoritmo
    def computeUserSuggestion_Naive(movies: RDD[Movie]) = {
        val users = this.users(movies)    //: RDD[(String, Iterable[(String, Double)])]

        val usersGraph = users.cartesian(users)

        val userRelatedMovies = usersGraph
            .filter({case (u1, u2) =>       // utenti 'simili' (e diversi da sé stesso)
                (u1._1 != u2._1) && similar(u1, u2).similar })
            .map({case (u1, u2) =>      // mantengo id di u1 e lista recensioni di u2
                (u1._1, u2._2)
            })
            .flatMapValues(x => x)
            .groupByKey()
        //: RDD[(String, List[(String, Double)])]

        userRelatedMovies.mapValues((relatedMovies) =>
            relatedMovies.groupBy(_._1)  // divide in base al film
                .map(group => {
                    // somma degli score per film
                    val sum = group._2.aggregate(0.0)(    // NOTA: 0 è il valore di default
                        (n : Double, mov : (String, Double)) => n + mov._2,       // come aggregare nella partizione
                        (n1 : Double, n2 : Double) => n1 + n2)             // come aggregare tra diverse partiz.
                    val avg = sum / group._2.size     // media per film
                    (group._1, avg)
                })
        ) //: RDD[(String, Iterable[(String, Double)])]
    }

    // cerca di migliorare riducendo la dimensione del cartesian tra utenti
    def computeUserSuggestion_ImprovedCartesian(movies: RDD[Movie]) = {
        val users = this.users(movies)     //: RDD[(String, Iterable[(String, Double)])]

        // IDEA: molti utenti hanno recensito un solo film; questi non ha senso confrontarli tra di loro
        // (o non sono similar, oppure lo sono ma non hanno consigli da darsi)
        val usersSingleMovie = users.filter(usr => usr._2.size == 1)
        val usersMoreMovies = users.filter(usr => usr._2.size > 1)

        val usersGraph = (usersSingleMovie.cartesian(usersMoreMovies)) union (usersMoreMovies.cartesian(usersMoreMovies))

        val userRelatedMovies = usersGraph
            .filter({case (u1, u2) =>       // utenti 'simili' (e diversi da sé stesso)
                (u1._1 != u2._1) && similar(u1, u2).similar })
            .map({case (u1, u2) =>      // mantengo id di u1 e lista recensioni di u2
                (u1._1, u2._2)
            })
            .flatMapValues(x => x)
            .groupByKey()
        //: RDD[(String, Iterable[(String, Double)])]

        userRelatedMovies.mapValues((relatedMovies) =>
            relatedMovies.groupBy(_._1)  // divide in base al film
                .map(group => {
                    // somma degli score per film
                    val sum = group._2.aggregate(0.0)(    // NOTA: 0 è il valore di default
                        (n : Double, mov : (String, Double)) => n + mov._2,       // come aggregare nella partizione
                        (n1 : Double, n2 : Double) => n1 + n2)             // come aggregare tra diverse partiz.
                    val avg = sum / group._2.size     // media per film
                    (group._1, avg)
                })
        ) //: RDD[(String, Iterable[(String, Double)])]
    }

    // invece di partire da tutte le coppie possibili di utenti, li associa a partire da un film comune
    def computeUserSuggestion_Optimized(movies: RDD[Movie]) = {
        val users = this.users(movies)
            .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // ad ogni film associa gli utenti che l'hanno visto
        val film_users_MovList = movies.map((mov) => (mov.userId, mov.productId))
            .join(users)
            .map {
                case (userId1, (filmId, movList)) =>
                    (filmId, (userId1, movList))
            }
            .persist(StorageLevel.MEMORY_AND_DISK_SER)

        // coppie di utenti con (almeno) un film in comune
        val userPairs = film_users_MovList.join(film_users_MovList)
            .map {
                case (filmId, ((userId, movList1),((userId2, movList2)))) =>
                    ((userId, movList1), (userId2, movList2))
            }

        // utenti 'simili' (e diversi da sé stesso)
        val usersGraph = userPairs.filter({
            case (u1, u2) =>
                (u1._1 != u2._1) && similar(u1, u2).similar
        })

        // mantengo id di u1 e lista recensioni di u2
        val userRelatedMovies = usersGraph.map({
                case (u1, u2) => (u1._1, u2._2)
            })
            .flatMapValues(x => x)
            .aggregateByKey(List[(String, Double)]()) ((x,y) => y::x, _++_)
        //: RDD[(String, List[Movie])]

        // ad ogni utente si prevede uno score  ai film associati pari alla media degli altri utenti 'simili'
        userRelatedMovies.mapValues((relatedMovies) =>
            relatedMovies.groupBy(_._1)  // divide in base al film
                .map(group => {
                    // somma degli score per film
                    val sum = group._2.aggregate(0.0)(    // NOTA: 0 è il valore di default
                        (n : Double, mov : (String, Double)) => n + mov._2,       // come aggregare nella partizione
                        (n1 : Double, n2 : Double) => n1 + n2)             // come aggregare tra diverse partiz.
                    val avg = sum / group._2.size     // media per film
                    (group._1, avg)
                })
        ) //: RDD[(String, Iterable[(String, Double)])]
    }
}
