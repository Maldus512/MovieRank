package movierank.operations

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import movierank.movies.Movie

/**
 * Correlazione tra lunghezza di una recensione e la sua utilità (helpfulness)
 */
object LengthHelpfulness {
    def compute(movies: RDD[Movie]) = {
        val STEP = 1024;

        // le recensioni vengono divise in "classi" in base al range di lunghezza, in base allo step
        //amovies.foreach( (m) => if (m.text.length >= 30000) println(m.text))
        val pairs = movies.filter(_.percentage != None).map(mov => (mov.text.length/STEP, mov.percentage.getOrElse(0)))

        // per ogni classe si calcola la helpfulness media
        val steps = pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
        //steps.foreach { case (step, value) => println(step*STEP, value._2)}
        val average = steps.map { case (step, value) => (step*STEP, value._1/value._2) }
        average.sortByKey().map{ case (step, value) => ( "< " + step.toString, value) }
    }
}
