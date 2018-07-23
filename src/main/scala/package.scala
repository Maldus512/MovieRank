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
}
