import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
import scala.collection._

object Project {
//  def calculateScore(score: String) : Double = score match{
//    case "A" => 4
//    case "A-" => 3.7
//    case "B+" => 3.3
//    case "B" => 3
//    case "B-" => 2.7
//    case "C+" => 2.3
//    case "C" => 2
//    case "C-" => 1.7
//    case "D+" => 1.3
//    case "D" => 1
//    case "F" => 0
//    case _ => 0
//  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Project").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val reviews = sc.textFile("src/main/scala/rotten_tomatoes_critic_reviews.txt")
    val movies = sc.textFile("src/main/scala/rotten_tomatoes_movies.txt")

    val moviesMap = movies.map(line =>
      (line.split("~~")(0), //movieId
        line.split("~~")(1), //Title
        line.split("~~")(2), //description
        line.split("~~")(3), //content rating
        line.split("~~")(4), //genre
        line.split("~~")(5), //actors
        line.split("~~")(6), //releaseDate
        line.split("~~")(7), //runtime
        line.split("~~")(8), //productionCompany
        line.split("~~")(9), //criticRating
        line.split("~~")(10) //audienceRating
      )
    )

    val reviewsMap = reviews.map(line =>
      (line.split("~~")(0), //movieId
        line.split("~~")(1), //criticName
        line.split("~~")(2), //topCritic
        line.split("~~")(3), //reviewScore
        line.split("~~")(4) //reviewContent
      ))

    // compute the average audience rating for movies made by the same production company
    val criticRating = moviesMap.map{case(movieId, title, description, cr, genre, actors, rdate, runtime, company, crating, arating)
       => (company, crating)}.filter { x => x._2 != "null" }
      .mapValues(v => (v.toInt, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues{ case(x,y) => x*1.0/y}
      .collect().foreach(println(_))

    // compute the average critics rating for each movie production company
    val audienceRating = moviesMap.map { case (movieId, title, description, cr, genre, actors, rdate, runtime, company, crating, arating)
      => (company, arating)}.filter { x => x._2 != "null" }
      .mapValues(v => (v.toInt, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues { case (x, y) => x * 1.0 / y }
      .collect().foreach(println(_))

    // the 10 movie production companies with the highest rating from audience
    println("movie companies with the highest audience rating: ")
    audienceRating.sortBy(r=>(r._2), false).take(10).foreach(println(_))

    // the 10 movie production companies with the highest rating from critics
    println("movie companies with the highest critics rating: ")
    criticRating.sortBy(r => (r._2), false).take(10).foreach(println(_))

    // the 10 movie production companies with the lowest rating from audience
    println("movie companies with the lowest audience rating: ")
    audienceRating.sortBy(r => (r._2), true).take(10).foreach(println(_))

    // the 10 movie production companies with the lowest rating from critics
    println("movie companies with the lowest critics rating: ")
    criticRating.sortBy(r => (r._2), true).take(10).foreach(println(_))

    // top 10 critics that wrote most reviews on movie
      reviewsMap.map { case (movieId, criticName, topCritic, reviewScore, reviewContent)
        => (criticName, 1) }.filter { x => x._1 != "null" }
        .reduceByKey{ case (x, y) => (x+y) }.sortBy(_._2, false)
        .collect.take(10).foreach(println(_))

    // top 10 top critics that wrote most reviews on movie
    reviewsMap.map { case (movieId, criticName, topCritic, reviewScore, reviewContent)
      => (criticName, topCritic, 1)
      }.filter{ x => x._1 != "null" }.filter{x => x._2 == "True"}
      .map{case(x, y ,z) => (x,z)}
      .reduceByKey { case (x, y) => (x + y) }
      .sortBy(_._2, false)
      .collect.take(10).foreach(println(_))

    // find the top 10 movie with the highest review score
//    reviewsMap.map { case (movieId, criticName, topCritic, reviewScore, reviewContent)
//      => (movieId, reviewScore)}.filter { x => x._2 != "null" }
//      .mapValues{(v=>calculateScore(v))}
//      .collect.foreach(println(_))
  }
}
