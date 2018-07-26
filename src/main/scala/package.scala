import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
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
}
