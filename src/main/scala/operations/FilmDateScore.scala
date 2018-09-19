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
 * Vedere come lo score di un film cambia in periodi differenti
 */
object FilmDateScore {

    def compute(movies: RDD[Movie], yearAggregate:Boolean = false):RDD[((String, String), Double)] = {

        // Analisi delle recenzioni anno per anno
        if (!yearAggregate) {   // I dati ritornati sono nella forma ((B00004CQT3,2009),5.0)
            val df = new SimpleDateFormat("yyyy")

            //map di ogni recenzione nella coppia ((id_film, anno_recenzione), score)
            val pairs = movies.map((mov) => ((mov.productId, df.format(mov.time*1000).toString()), mov.score))

            //la aggregateByKey mette insieme tutte le recenzioni dello stesso film fatte nello stesso anno
            //e somma gli score dati in ogni recenzioni per poi fare la media
            pairs.aggregateByKey((0.0,0)) ((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
                .map { case (filmId, score) => (filmId, score._1/score._2) }
        }
        // Analisi delle recensioni raggruppate per range fissato di anni (1997 - 2002) (2003 - 2007) (2008 - 2012)
        else {   // I dati ritornati sono nella forma ((B006JIUN2W,2008-2012),5.0)
            val format = new SimpleDateFormat("dd-MM-yyyy")
            //vengono definiti le date per i range
            val date1 = format.parse("01-01-2003").getTime()/ 1000
            val date2 = format.parse("01-01-2008").getTime()/ 1000

            /*  range 1997-2002 viene definito range 0
             *  range 2003-2007 viene definito range 1
             *  range 2008-2012 viene definito range 2
            */
            val pairs = movies.map((mov) =>
                                    if (mov.time <= date1)
                                        ((mov.productId, 0), mov.score)
                                    else if (mov.time > date2)
                                        ((mov.productId, 2), mov.score)
                                    else
                                        ((mov.productId, 1), mov.score)
                                )
                                .mapValues((_, 1))  //aggiunto questo valore insieme allo score al fine di fare la somma e la media

            //la reduceByKey mette insieme gli stessi film recensiti nel medesimo range (0,1,2)
            pairs.reduceByKey{ case ((score1, count1), (score2, count2)) => (score1 + score2, count1 + count2) }
                    .map{ case ((key, slot), (score, count)) =>
                            if (slot == 0)
                                ((key, "1997-2002"), score / count)
                            else if (slot == 1)
                                ((key, "2003-2007"), score / count)
                            else
                                ((key, "2008-2012"), score / count)
                    }
        }
    }
}
