package movierank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import java.io._
import movierank.movies.Movie
import movierank.operations.{FilmScore, UserScore, UserHelpfulness, LengthHelpfulness, FilmDateScore, PageRank, UserSuggestion }

object Main {
    def main(args: Array[String]) = {
        //"s3a://simple-spark-deploy-bucket-zavy/movies.txt"
        val path: String =args(0)

        //configura Spark
        val conf = new SparkConf()
           .setAppName("SparkJoins")
           .setMaster("local")
           .set("spark.hadoop.validateOutputSpecs", "false")

        val context = new SparkContext(conf)

        val movies = load(path, context)

        val t0 = System.nanoTime()

        //FilmScore.compute(movies, context).foreach(println)
        //UserHelpfulness.compute(movies, context).foreach(println)
        //FilmDateScore.compute(movies, context).foreach(println)
        //UserScore.compute(movies, context).foreach(println)
        //val result = PageRank.compute(movies, context).foreach(println)
        val result = UserSuggestion.compute(movies, context).foreach(println)


        /*val result = FilmDateScore.compute(movies, context)
        result.foreach(println)
        FilmDateScore.toChart(result)*/

        deleteRecursively(new File("./tmp/out"))
        result.saveAsTextFile("./tmp/out")

        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
        context.stop()
    }
}
