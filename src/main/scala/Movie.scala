package movierank.movies

import scala.collection.Map

class Movie (ms : Map[String, String]) extends Serializable {
    val productId : String= ms getOrElse ("productId", "")
    val userId :String = ms getOrElse ("userId", "")
    val profileName : String = ms getOrElse ("profileName", "")
    val utilita :String = ms getOrElse ("helpfulness", "0")
    val score: Float = ms getOrElse ("score", "0.0") toFloat
    val time : Long = ms getOrElse ("time", "0") toLong
    val summary : String = ms getOrElse ("summary", "")
    val text : String = ms getOrElse("text", "")
    override def toString = "Product: " + productId +
        "\n\t summary: \t" + summary +
        "\n\t score: \t" + score.toString
}
