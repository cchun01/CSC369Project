import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Project {
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

  }
}
