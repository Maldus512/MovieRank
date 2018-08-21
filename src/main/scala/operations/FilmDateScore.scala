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
import movierank.operations.Operation
import vegas._
import vegas.render.WindowRenderer._

object FilmDateScore{

    def compute(movies: RDD[Movie], context: SparkContext, yearAggregate:Boolean = false):RDD[((String, String), Double)] = {
        if (!yearAggregate){    // I dati ritornati sono nella forma ((B00004CQT3,2009),5.0)
            val df = new SimpleDateFormat("yyyy")

            val pairs = movies.map((mov) => ((mov.productId, df.format(mov.time*1000).toString()), mov.score))

            pairs.aggregateByKey((0.0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
                .map { case (filmId, score) => (filmId, score._1/score._2) }
        }
        else{                   // I dati ritornati sono nella forma ((B006JIUN2W,2008-2012),5.0)
            val format = new SimpleDateFormat("dd-MM-yyyy")
            val date1 = format.parse("01-01-2003").getTime()/ 1000
            val date2 = format.parse("01-01-2008").getTime()/ 1000

            val pairs = movies.map((mov) =>
                                    if (mov.time <= date1)
                                        ((mov.productId, 0), mov.score)
                                    else if (mov.time > date2)
                                        ((mov.productId, 2), mov.score)
                                    else
                                        ((mov.productId, 1), mov.score)
                                )

                .mapValues((_, 1))

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

    // I dati passati sono nella forma ((B00004CQT3,2009),5.0)
    def toChart(data: RDD[((String, String), Double)]) = {
        var dataToChart = (data map {element => collection.immutable.Map("Symbol" -> element._1._1, "Period" -> element._1._2, "Score" -> element._2)}).collect()

        val plot = Vegas("Prova1").
            withData(dataToChart).
            mark(Line).
            encodeX("Period", Nominal).
            encodeY("Score", Quant).
            encodeColor(field="Symbol", dataType=Nominal, legend=Legend(orient="left", title="Symbol")).
            encodeDetailFields(Field(field="symbol", dataType=Nominal)).
            show
    }
}
