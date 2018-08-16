import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import java.io._
import movierank.movies.Movie

package object movierank {
    def load(path:String, context:SparkContext) = {
        val hconf = new org.apache.hadoop.mapreduce.Job().getConfiguration
        hconf.set("textinputformat.record.delimiter", "\n\n")

        //crea RDD dove splitta ogni movie dove ci sono le righe vuote
        val usgRDD = context.newAPIHadoopFile(
          path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hconf)
        .map({ case (_, v) => v.toString })

        //crea RDD dove ogni movie è una sequenza di stringhe
        val blocks: RDD[Seq[String]] = usgRDD.map(_.split("\n"))

        //crea RDD dove ogni movie è un dizionario (key,value)
        blocks.map((xs :Seq[String]) => {
            val values = Map[String, String]()
            val res = xs.foldLeft(values)((ms: Map[String,String], element:String) => {
                val k = element.split(':')(0).split('/')(1)
                val tmp = element.split(':')
                val v = tmp.slice(1, tmp.size).foldLeft("")((a, x) => a+x.trim)
                ms.updated(k,v)
            })
            new Movie(res)
        })
    }

    def pageRank(edges: RDD[(String, String)], initialValues: RDD[(String, Double)]) : RDD[(String, Double)] = {
        val tmplinks = edges.groupByKey()
        var ranks = tmplinks.join(initialValues)
                        .mapValues { case (nodes, value) => value }
        val links = tmplinks.join(initialValues)
                        .mapValues { case (nodes, value) => nodes }
                        .persist()
        for(i <- 0 until 10) {
            val contributions = links.join(ranks).flatMap {
                case (u, (uLinks, urank)) =>
                uLinks.map(t => (t, urank / uLinks.size))
            }
            ranks = contributions.
                reduceByKey((x,y) => x+y).
                mapValues(v => 0.15+0.85*v)
        }
        ranks
    }

    def userHelpfulness(movies: RDD[Movie]) : RDD[(String, Option[Double])] = {
        val pairs = movies.map((mov) => (mov.userId, mov.helpfulness))
            .mapValues((helpfulness) => (helpfulness.split("/")(0).toInt, helpfulness.split("/")(1).toInt))
        pairs.reduceByKey{ case ((score_pos1, score_tot1), (score_pos2, score_tot2)) => (score_pos1 + score_pos2, score_tot1 + score_tot2) }
            .mapValues{ case (score_pos, score_tot) => (if (score_tot.toInt == 0) None else Some((score_pos.toDouble / score_tot.toDouble)*100)) }
    }

    def helpfulnessByScore(movies: RDD[Movie], productId:String) = {
             // Coppie valutazione del film - helpfulness della review
        val pairs = movies.filter( mov => mov.productId == productId  && !mov.percentage.isEmpty).map( mov => (mov.score, mov.percentage.get) )
        
        // Helpfulness media delle review per film in base allo score assegnato
        pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
            .map { case (score, help) => (score, help._1/help._2) }

    }

    def helpfulnessByScore(movies: RDD[Movie]) = {
             // Coppie valutazione del film - helpfulness della review
        val pairs = movies.filter(mov => !mov.percentage.isEmpty).map( mov => ((mov.score, mov.productId), mov.percentage.get) )
        
        // Helpfulness media delle review per film in base allo score assegnato
        pairs.aggregateByKey((0,0)) ((acc, value) => (acc._1+value, acc._2+1), (acc1,acc2) => (acc1._1 + acc2._1, acc1._2+ acc2._2))
            .map { case (score, help) => (score, help._1/help._2) }
    }

    def deleteRecursively(file: File): Unit = {
        if (file.isDirectory)
            file.listFiles.foreach(deleteRecursively)
        if (file.exists && !file.delete)
            throw new Exception(s"Unable to delete ${file.getAbsolutePath}")   
    }
}
