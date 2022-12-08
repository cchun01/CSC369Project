import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Project {

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

  def euclideanDistance(xScore: Double, yScore: Double, xRating: List[Int], yRating: List[Int], xRuntime: Double, yRuntime: Double): Double = {
    var dist = 0.0
    val ratingsMap = xRating.zip(yRating)
    val vectorDist = ratingsMap.map{
      case(ui, vi) => Math.pow(ui - vi, 2.0)
    }.sum

    dist += 0.7 * Math.pow(xScore - yScore, 2.0)
    dist += 0.1 * Math.pow(xRuntime - yRuntime, 2.0)
    dist += 0.2 * vectorDist
    Math.sqrt(dist)
  }

  def getSimilarMoviesAndRating(trainScaled: RDD[(String, Double, Double, List[Int])],
                                testScaled2: RDD[(String, Double, Double, List[Int])],
                                withAudienceRatings: RDD[(String, Iterable[(String, String, String, String)])],
                                testWithAudienceRatings: RDD[(String, Iterable[(String, String, String, String)])]): (Double, Double, Double)= {
    val euclidDistMap = trainScaled.cartesian(testScaled2).map({case ((id1, a, c, b), (id2, x, z, y)) => (id1+", "+id2, euclideanDistance(a, x, b, y, c, z))}).sortBy(_._2).take(5)
    val testMovieID = euclidDistMap.toList.head._1.split(", ")(1).trim()
    println("For Movie: " + testMovieID)
    println()
    println("Similar Movies: ")
    euclidDistMap.foreach(x => println(x._1 + " " + x._2))
    println()
    val predictedAudienceRatingList = euclidDistMap.map(x=>withAudienceRatings.lookup(x._1.split(",")(0).trim()).toList.head.toList.head._4.toDouble)
    val predictedAudienceRating = predictedAudienceRatingList.sum/predictedAudienceRatingList.length
    println("Predicted Rating: " + predictedAudienceRating)
    (testWithAudienceRatings.lookup(testMovieID).toList.head.toList.head._4.toDouble, predictedAudienceRating, Math.abs(testWithAudienceRatings.lookup(testMovieID).toList.head.toList.head._4.toDouble - predictedAudienceRating))
  }

  def getDataParallelized(data: RDD[(String, String, String, List[Int])],
                          scoreMean: Double,
                          scoreSD: Double,
                          runtimeMean: Double,
                          runtimeSD: Double): RDD[(String, Double, Double, List[Int])] = {
    data.map({case (id, x,z, y) => (id, (x.toDouble - scoreMean)/scoreSD, (z.toDouble - runtimeMean)/runtimeSD, y)})
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Project").setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    val practiceMovies = sc.textFile("src/main/scala/practiceTrain.txt")
    val testMovies = sc.textFile("src/main/scala/smallerTest.txt")
    val scoreAndRating = practiceMovies.map(line => (line.split("~~")(0),line.split("~~")(3), line.split("~~")(7), line.split("~~")(9), line.split("~~")(10))).filter(x=>x._1 != "null" && x._2 != "null" && x._3 != "null" && x._4 != "null" && x._5 != "null")
    val testScoreAndRating = testMovies.map(line => (line.split("~~")(0),line.split("~~")(3), line.split("~~")(7), line.split("~~")(9), line.split("~~")(10))).filter(x=>x._1 != "null" && x._2 != "null" && x._3 != "null" && x._4 != "null" && x._5 != "null")
    val scoreAndRatingGroup = practiceMovies.map(line => (line.split("~~")(0),(line.split("~~")(3), line.split("~~")(7), line.split("~~")(9), line.split("~~")(10)))).filter(x=>x._1 != "null" && x._2._1 != "null" && x._2._2 != "null" && x._2._3 != "null" && x._2._4 != "null")
    val testScoreAndRatingGroup = testMovies.map(line => (line.split("~~")(0),(line.split("~~")(3), line.split("~~")(7), line.split("~~")(9), line.split("~~")(10)))).filter(x=>x._1 != "null" && x._2._1 != "null" && x._2._2 != "null" && x._2._3 != "null" && x._2._4 != "null")
    val withAudienceRatings = scoreAndRatingGroup.groupByKey()
    val testWithAudienceRatings = testScoreAndRatingGroup.groupByKey()

    val scoreAndRatingMapped = scoreAndRating.map({case (id, x, z, y, pred) => (id, y, mapCR(x), z)}).map({case (id, score, rating, runtime) => (id, score, runtime, List(crToCROneHot(rating, 0),crToCROneHot(rating, 1),crToCROneHot(rating, 2),crToCROneHot(rating, 3),crToCROneHot(rating, 4),crToCROneHot(rating, 5)))})
    val scoreMean = (scoreAndRating.map({case (id, x, z, y, pred) => y.toInt}).collect().sum)*1.0/scoreAndRating.count()
    val scoreSD = Math.sqrt((scoreAndRating.map({case (id, x, z, y, pred) => (y.toInt*1.0 - scoreMean)*(y.toInt*1.0 - scoreMean)}).collect().sum)*1.0/scoreAndRating.count())

    val runtimeMean = (scoreAndRating.map({ case (id, x, z, y, pred) => z.toInt }).collect().sum) * 1.0 / scoreAndRating.count()
    val runtimeSD = Math.sqrt((scoreAndRating.map({ case (id, x, z, y, pred) => (z.toInt * 1.0 - runtimeMean) * (z.toInt * 1.0 - runtimeMean) }).collect().sum) * 1.0 / scoreAndRating.count())

    val trainScaled = scoreAndRatingMapped.map({case (id, x,z,y) => (id, (x.toDouble - scoreMean)/scoreSD, (z.toDouble - scoreMean)/scoreSD, y)})

    val testDataCleaned = testScoreAndRating.map({case (id, x, z, y, pred) => (id, y, mapCR(x), z)}).map({case (id, score, rating, runtime) => (id, score, runtime, List(crToCROneHot(rating, 0),crToCROneHot(rating, 1),crToCROneHot(rating, 2),crToCROneHot(rating, 3),crToCROneHot(rating, 4),crToCROneHot(rating, 5)))}).collect()

    var i = 0
    var testDataCleaned2 = new ListBuffer[RDD[(String, String, String, List[Int])]]()

    for (i <- testDataCleaned.indices) {
      testDataCleaned2 += sc.parallelize(List(testDataCleaned(i)))
    }

    var trueAndPred = new ListBuffer[(Double, Double, Double)]()
    val testDataCleaned3 = testDataCleaned2.map(x => getDataParallelized(x, scoreMean, scoreSD, runtimeMean, runtimeSD))
    testDataCleaned3.foreach(x=>trueAndPred += getSimilarMoviesAndRating(trainScaled, x, withAudienceRatings, testWithAudienceRatings))

    val mse = trueAndPred.map({case(x,y,z)=> Math.pow(x-y, 2)}).sum/trueAndPred.length
    val avgDiff = trueAndPred.map({case(x,y,z)=> z}).sum/trueAndPred.length
    println()
    println("MSE for Audience Rating Predictions: " + mse)
    println("Predictions for audience ratings are off by " + avgDiff + " on average.")
  }
}
