package movierank

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import java.io._
import movierank.movies.Movie
import movierank.operations.{FilmScore, UserScore, UserHelpfulness, LengthHelpfulness, FilmDateScore, PageRank, UserSuggestion }

object Main {
    def main(args: Array[String]) = {
        //val path: String = "s3a://movierank-deploy-bucket/movies500m.txt"
        val path: String =args(0)
        val algorithm: String = args(1)
        val saveMode: String = args(2)

        //configura Spark
        val conf = new SparkConf()
           .setAppName("SparkJoins")
           //.setMaster("local[4]")
           .set("spark.hadoop.validateOutputSpecs", "false")

        val context = new SparkContext(conf)

        val t0_total = System.nanoTime()

        val movies = load(path, context)
        movies.persist(StorageLevel.MEMORY_AND_DISK_SER)

        val t0 = System.nanoTime()

        var result = algorithm match {
            case "pagerank_averageI" => PageRank.computePageRank_averageInefficient(movies, context)
            case "pagerank_average" => PageRank.computePageRank_average(movies, context)
            case "pagerank_naive" => PageRank.computePageRank_Naive(movies, context)
            case "pagerank_medium" => PageRank.computePageRank_noCartesian(movies, context)
            case "pagerank_optimize" => PageRank.computePageRank_Optimize(movies, context)
            case "usersuggestion" => UserSuggestion.compute(movies, context)
            case "filmdatescore" => FilmDateScore.compute(movies, context)
            case "filmscore" => FilmScore.compute(movies, context)
            case "lengthhelpfulness" => LengthHelpfulness.compute(movies, context)
            case "userscore" => UserScore.compute(movies, context)
        }
        result.count()

        val t1 = System.nanoTime()
        //result.coalesce(1, true).saveAsTextFile("s3a://movierank-deploy-bucket/"+algorithm+path)

        saveMode match {
            case "local" => {
                val data = result.collect().map { case (x,y) => Array(x.toString, y.toString)}
                val t1_total = System.nanoTime()
                save(algorithm, data.toList
                                ++ List(Array("Elapsed time: " + (t1 - t0)/1000000 + "ms"))
                                ++ List(Array("Total elapsed time: " + (t1_total - t0_total)/1000000 + "ms"))
                                ++ List(Array("Total collect time: " + (t1_total - t1)/1000000 + "ms")))
            }
            case "distributed" => {
                result.saveAsTextFile("/tmp/out/")
            }
            case "s3" => {
                result.saveAsTextFile("s3a://movierank-deploy-bucket/out/")
            }
            case _ : String => {}
        }

        val t1_total = System.nanoTime()
        println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
        println("Total elapsed time: " + (t1_total - t0_total)/1000000 + "ms")
        println("Total collect time: " + (t1_total - t1)/1000000 + "ms")
        context.stop()
    }
}
