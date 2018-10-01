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

/**
 * Correlazione tra anno della recenzione e film recensito
 * L'obiettivo è vedere come lo score di un film cambia in periodi differenti
 */
object FilmDateScore {

    // Analisi delle recenzioni anno per anno
    // I dati ritornati sono nella forma ((B00004CQT3,2009),5.0)
    def computeFilmDateScore( movies: RDD[Movie], id_movie:String = null ):RDD[((String, String), Double)] = {
        var movies_init = movies
        if (id_movie != null){
                movies_init = movies.filter((mov) => mov.productId == id_movie)
        }
        val df = new SimpleDateFormat("yyyy")

        // viene mappata ogni recenzione nella coppia ((id_film, anno_recenzione), score)
        val pairs = movies_init.map((mov) => ((mov.productId, df.format(mov.time*1000).toString()), mov.score))

        // aggregateByKey mette insieme tutte le recenzioni dello stesso film fatte nello stesso anno
        // sommando gli score per poi fare la media
        pairs.aggregateByKey((0.0,0)) ((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
            .map { case (filmId, score) => (filmId, (((score._1/score._2) * 100).round / 100.toDouble)) }   // i valori vengono arrotondati a due cifre decimali
    }

    // Analisi delle recensioni raggruppate per range fissato di anni (1997 - 2002) (2003 - 2007) (2008 - 2012)
    // I dati ritornati sono nella forma ((B006JIUN2W,2008-2012),5.0)
    def computeFilmDateScoreAggregated( movies: RDD[Movie], id_movie:String = null ):RDD[((String, String), Double)] = {
        var movies_init = movies
        if (id_movie != null){
                movies_init = movies.filter((mov) => mov.productId == id_movie)
        }
        val format = new SimpleDateFormat("dd-MM-yyyy")
        //vengono definiti le date per i range
        val date1 = format.parse("01-01-2003").getTime()/ 1000
        val date2 = format.parse("01-01-2008").getTime()/ 1000

        /*  range 1997-2002 viene definito range 0
         *  range 2003-2007 viene definito range 1
         *  range 2008-2012 viene definito range 2
        */
        val pairs = movies_init.map((mov) =>
                                if (mov.time <= date1)
                                    ((mov.productId, 0), mov.score)
                                else if (mov.time > date2)
                                    ((mov.productId, 2), mov.score)
                                else
                                    ((mov.productId, 1), mov.score)
                            )
                            .mapValues((_, 1))  //aggiunto questo valore insieme allo score al fine di fare la somma e la media

        // reduceByKey mette insieme le recenzioni degli stessi film recensiti nel medesimo range (0,1,2)
        pairs.reduceByKey{ case ((score1, count1), (score2, count2)) => (score1 + score2, count1 + count2) }
                .map{ case ((key, slot), (score, count)) =>
                        if (slot == 0)
                            ((key, "1997-2002"), (((score / count) * 100).round / 100.toDouble))    // i valori vengono arrotondati a due cifre decimali
                        else if (slot == 1)
                            ((key, "2003-2007"), (((score / count) * 100).round / 100.toDouble))    // i valori vengono arrotondati a due cifre decimali
                        else
                            ((key, "2008-2012"), (((score / count) * 100).round / 100.toDouble))   // i valori vengono arrotondati a due cifre decimali
                }
    }
}
