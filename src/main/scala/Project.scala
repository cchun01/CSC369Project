import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Project {
<<<<<<< Updated upstream

  def mapCR(x: String): Int = {
    x match {
      case "G" =>
        0
      case "PG" =>
        1
      case "PG-13" =>
        2
      case "R" =>
        3
      case "NC17" =>
        4
      case "NR" =>
        5
    }
  }

  def crToCROneHot(x : Int, y : Int): Int = {
    if (x == y)
      1
    else
      0
  }

  def euclideanDistance(xScore: Double, yScore: Double, xRating: List[Int], yRating: List[Int]): Double = {
    var dist = 0.0
    val ratingsMap = xRating.zip(yRating)
    val vectorDist = ratingsMap.map{
      case(ui, vi) => Math.pow(ui - vi, 2.0)
    }.sum
    println(vectorDist)
    dist += Math.pow(xScore - yScore, 2.0)
    dist += vectorDist
    Math.sqrt(dist)
  }


=======
>>>>>>> Stashed changes
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Project").setMaster("local[1]")
    val sc = new SparkContext(conf)
<<<<<<< Updated upstream

    val reviews = sc.textFile("src/main/scala/rotten_tomatoes_critic_reviews.txt")
    val movies = sc.textFile("src/main/scala/rotten_tomatoes_movies.txt")

//    val moviesMap = movies.map(line =>
//      (line.split("~~")(0), //movieId
//        line.split("~~")(1), //Title
//        line.split("~~")(2), //description
//        line.split("~~")(3), //content rating
//        line.split("~~")(4), //genre
//        line.split("~~")(5), //actors
//        line.split("~~")(6), //releaseDate
//        line.split("~~")(7), //runtime
//        line.split("~~")(8), //productionCompany
//        line.split("~~")(9), //criticRating
//        line.split("~~")(10) //audienceRating
//      )
//    )

//    val reviewsMap = reviews.map(line =>
//      (line.split("~~")(0), //movieId
//        (line.split("~~")(1), //criticName
//        line.split("~~")(2), //topCritic
//        line.split("~~")(3), //reviewScore
//        line.split("~~")(4)) //reviewContent
//      ))

//    val contentRatings = movies.map(line => line.split("~~")(3))
//    val criticScore = movies.map(line => line.split("~~")(9))
    val practiceMovies = sc.textFile("src/main/scala/practiceTrain.txt")
    val scoreAndRating = practiceMovies.map(line => (line.split("~~")(0),line.split("~~")(3), line.split("~~")(9)))

    val scoreAndRatingMapped = scoreAndRating.map({case (id, x,y) => (id, y, mapCR(x))}).map({case (id, score, rating) => (id, score, List(crToCROneHot(rating, 0),crToCROneHot(rating, 1),crToCROneHot(rating, 2),crToCROneHot(rating, 3),crToCROneHot(rating, 4),crToCROneHot(rating, 5)))})
    val scoreMean = (scoreAndRating.map({case (id, x, y) => y.toInt}).collect().sum)*1.0/scoreAndRating.count()
    val scoreSD = Math.sqrt((scoreAndRating.map({case (id, x, y) => (y.toInt*1.0 - scoreMean)*(y.toInt*1.0 - scoreMean)}).collect().sum)*1.0/scoreAndRating.count())

    val trainScaled = scoreAndRatingMapped.map({case (id, x,y) => (id, (x.toDouble - scoreMean)/scoreSD, y)})

    val testScaled = sc.parallelize(List(("m/10008607-day_of_the_dead",13, List(0, 0, 0, 1, 0, 0))))
    val testScaled2 = testScaled.map({case (id, x,y) => (id, (x.toDouble - scoreMean)/scoreSD, y)})

    val euclidDistMap = trainScaled.cartesian(testScaled2).map({case ((id1, a, b), (id2, x, y)) => (id1+", "+id2, euclideanDistance(a, x, b, y))}).sortBy(_._2)
    euclidDistMap.foreach(x => println(x._1 + " " + x._2))
=======

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

>>>>>>> Stashed changes

    val criticMap = reviewsMap.map(line => (line._2, line._5.toLowerCase().replaceAll("[^a-zA-Z0-9]", " ").replaceAll("""\s+""", " ").split(" ").mkString(" "))).filter(_._1  != "null")

    val criticDesc = criticMap.reduceByKey(_+ " " +_)

    val descLst = criticDesc.map(line => (line._1, line._2.toString.split(" ").distinct.size, line._2.toString.split(" ").size))

    val topCount = descLst.map(x => (x._1, x._2)).sortBy(_._2, false).take(10)

    val avgDesc = descLst.map(x => (x._1, x._2.toDouble/x._3))

    val topRatio = avgDesc.filter(_._2 != 1.0).sortBy(_._2, false).take(10)

    println("Top 10 Distinct Word Count Per Critic")
    topCount.foreach(x => println(x._1 + ": " + x._2 + " words"))

    println()
    println("Top 10 Distinct to Word Ratio")
    topRatio.foreach(x => println(x._1 + ": " + x._2 + " words"))

    //    println()
    //    println("For Distinct Word Count Per Critic")
    //    descLst.collect().foreach(x => println(x._1 + ": " + x._2 + " words out of " + x._3))
    //
    //    println()
    //    println("Critic Distinct Count Compared to All Critics")
    //    avgDesc.collect().foreach(x => println("Critic " + x._1 + " Score: " + x._2))
  }
}
