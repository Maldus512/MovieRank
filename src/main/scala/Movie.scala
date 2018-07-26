package movierank.movies

import scala.collection.Map

class Movie (ms : Map[String, String]) extends Serializable {
    val productId : String= ms getOrElse ("productId", "")
    val userId :String = ms getOrElse ("userId", "")
    val profileName : String = ms getOrElse ("profileName", "")
    val helpfulness :String = ms getOrElse ("helpfulness", "0/0")
    val score: Float = ms getOrElse ("score", "0.0") toFloat
    val time : Long = ms getOrElse ("time", "0") toLong
    val summary : String = ms getOrElse ("summary", "")
    val text : String = ms getOrElse("text", "")

    val upvotes : Int = helpfulness.split("/")(0) toInt
    val total : Int = helpfulness.split("/")(1) toInt
    val downvotes : Int = total -upvotes

    val percentage : Option[Int] = if (total == 0) None else Some(upvotes*100/total)

    override def toString = "Product: " + productId +
        "\n\t user id: \t" + userId +
        "\n\t score: \t" + score.toString +
        "\n\t helpfulness \t" + helpfulness.toString +
        "\n\t summary: \t" + summary +
        "\n\t " + text.slice(0, math.min(128, text.size)) + "...\n"
}
