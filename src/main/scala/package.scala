import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import java.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import movierank.movies.Movie

package object movierank {

    /**
     * Carica il dataset da file e lo inserisce in un RDD
     */
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
                try  {
                    val k = element.split(':')(0).split('/')(1)
                    val tmp = element.split(':')
                    val v = tmp.slice(1, tmp.size).foldLeft("")((a, x) => a+x.trim)
                    ms.updated(k,v)
                } catch {
                    case _ : Throwable => ms
                }
            })
            new Movie(res)
        })
    }

    /**
     * Salva i dati su un file CSV predefinito
     */
    def save(name: String, data: List[Array[String]]) {
        val outputFile = new BufferedWriter(new FileWriter("/tmp/"+name+".csv")) //replace the path with the desired path and filename with the desired filename
        val csvWriter = new CSVWriter(outputFile)
        csvWriter.writeAll(data)
        outputFile.close()
    }

    /**
     * Rappresenta un arco tra due utenti
     *  - similar = true se sono considerati 'simili'
     */
    class SimilarityEdge  (var userId1 : String, var helpfulnessId1 : Double, var userId2 : String, var similar : Boolean, var degree : Double, var positiveEdge : Boolean, var helpfulnessDifference : Double) extends Serializable {
        override def toString = {
            s"userId1 : ${this.userId1}, userId2 : ${this.userId2}, similar : ${this.similar}, degree : ${this.degree}"
        }
    }

    /**
     * Crea un SimilarityEdge tra 2 utenti confrontando i film comuni ed i relativi score (per UserSuggestion)
     */
    def similar(x: (String, Iterable[(String, Double)]), y: (String, Iterable[(String, Double)])) : SimilarityEdge = {
        val (xId, xs) = x
        val (yId, ys) = y

        /* Intersezioni dei film visti da xId e yId */
        val commonMovies = xs.filter((x) =>
            ys.filter((y) => y._1 == x._1)
                .isEmpty == false
        )

        val differences = commonMovies.map((x) => {
            val score1 = x._2
            val score2 = ys.filter((y) => y._1 == x._1).head._2
            (score1 - score2).abs
        })

        val similarity = if(differences.isEmpty) 5.0
            else (differences.reduce((a,b) => a+b).toDouble / differences.size)

        new SimilarityEdge(xId, 0.0, yId, similarity <= 0.0, similarity, true, 0.0)
    }

    /**
     * Crea un SimilarityEdge tra 2 utenti memorizzando anche la Helpfulness (per PageRank)
     */
    def similarWithHelpfulness(x: (String, (Iterable[(String, Double)], Double)), y: (String, (Iterable[(String, Double)], Double))) : SimilarityEdge = {
        val (xId, (xs, helpfulness_X)) = x
        val (yId, (ys, helpfulness_Y)) = y

        // Intersezioni dei film visti da xId e yId
        val commonMovies = xs.filter((x) =>
            ys.filter((y) => y._1 == x._1)
                .isEmpty == false
        )

        val differences = commonMovies.map((x) => {
            val score1 = x._2
            val score2 = ys.filter((y) => y._1 == x._1).head._2
            (score1 - score2).abs
        })

        val similarity = if(differences.isEmpty) 5.0
            else (differences.reduce((a,b) => a+b).toDouble / differences.size)

        val helpfulness_difference = (helpfulness_Y - helpfulness_X)
        new SimilarityEdge(xId, helpfulness_X, yId, similarity <= 0.0, similarity, helpfulness_difference > 0, helpfulness_difference)
    }

}
