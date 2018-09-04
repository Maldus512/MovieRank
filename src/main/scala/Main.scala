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

        //configura Spark
        val conf = new SparkConf()
           .setAppName("SparkJoins")
           .setMaster("local")
           .set("spark.hadoop.validateOutputSpecs", "false")

        val context = new SparkContext(conf)

        val t0_total = System.nanoTime()

        val movies = load(path, context)
        movies.persist(StorageLevel.MEMORY_AND_DISK_SER)

        val t0 = System.nanoTime()

        var result = algorithm match {
            case "pagerankM" => PageRank.computePageRankM(movies, context)
            case "pagerankF" => PageRank.computePageRankF(movies, context)
            case "pagerankI" => PageRank.computePageRankI(movies, context)
            case "usersuggestion" => UserSuggestion.compute(movies, context)
        }

        val t1 = System.nanoTime()
        //result.coalesce(1, true).saveAsTextFile("s3a://movierank-deploy-bucket/"+algorithm+path)

        val data = result.collect().map { case (x,y) => Array(x.toString, y.toString)}
        val t1_total = System.nanoTime()
        save(algorithm, data.toList 
                        ++ List(Array("Elapsed time: " + (t1 - t0)/1000000 + "ms"))
                        ++ List(Array("Total elapsed time: " + (t1_total - t0_total)/1000000 + "ms"))
                        ++ List(Array("Total collect time: " + (t1_total - t1)/1000000 + "ms")))
        println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
        println("Total elapsed time: " + (t1_total - t0_total)/1000000 + "ms")
        println("Total collect time: " + (t1_total - t1)/1000000 + "ms")
        context.stop()
    }
}
