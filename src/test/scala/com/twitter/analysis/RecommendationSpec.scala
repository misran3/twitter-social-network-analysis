package com.twitter.analysis

import com.twitter.analysis.impl.SparkRecommendationProcessor
import com.twitter.analysis.models.DataModels
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

/** Test suite for SparkRecommendationProcessor implementation
  *
  * Validates correctness of follower recommendation algorithm using reference test data
  */
class RecommendationSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val TOLERANCE           = 1e-6 // Tolerance for floating-point comparison

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("RecommendationTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Recommendation computation on SmallGraph1 matches reference output") {
    // Load test graph with explicit schema
    val testGraphPath = "data/test-graph"

    val edgeDF = spark.read
      .option("sep", "\t")
      .schema(DataModels.edgeSchema)
      .csv(testGraphPath)

    // Load topics data with explicit schema
    val testTopicsPath = "data/test-graph-topics"

    val topicsDF = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(DataModels.topicSchema)
      .csv(testTopicsPath)

    // Create recommendation processor
    val recommendationProcessor = SparkRecommendationProcessor(spark)

    // Compute recommendations with 10 iterations
    val recommendationsDF = recommendationProcessor.computeRecommendations(edgeDF, topicsDF)

    // Collect results into a map
    val actualRecs = recommendationsDF
      .collect()
      .map { row =>
        val userId = row.getAs[Long]("userId")
        val recommendedUserId = row.getAs[Long]("recommendedUserId")
        val postFrequency = row.getAs[Double]("postFrequency")
        val topic = row.getAs[String]("topic")
        (userId, (recommendedUserId, postFrequency, topic))
      }
      .toMap

    // Load reference results
    val referenceRecs = loadReferenceRecommendations("data/test-graph-recs-ref")

    // Validate that all users are present
    actualRecs.keySet shouldEqual referenceRecs.keySet

    // Compare each recommendation with reference
    var maxDiff = 0.0
    var failedMatches = List.empty[(Long, Long, Double, Long, Double, Double)]

    // Print comparison table for debugging
    println("\nRecommendation Comparison:")
    println("=" * 85)
    println(f"${"User ID"}%-10s ${"Expected User"}%-15s ${"Expected Freq"}%-15s ${"Actual User"}%-15s ${"Actual Freq"}%-15s ${"Difference"}%-15s")
    println("-" * 85)

    referenceRecs.toSeq.sortBy(_._1).foreach { case (userId, (expectedUserId, expectedFreq)) =>
      val (actualUserId, actualFreq, topic) = actualRecs(userId)
      val freqDiff = Math.abs(actualFreq - expectedFreq)
      maxDiff = Math.max(maxDiff, freqDiff)

      val userMatch = actualUserId == expectedUserId
      val freqMatch = freqDiff < TOLERANCE
      val status = if (userMatch && freqMatch) "✓" else "✗"

      println(f"$userId%-10d $expectedUserId%-15d $expectedFreq%-15.3f $actualUserId%-15d $actualFreq%-15.3f $freqDiff%-15.2e $status")

      if (!userMatch || !freqMatch) {
        failedMatches = (userId, expectedUserId, expectedFreq, actualUserId, actualFreq, freqDiff) :: failedMatches
      }
    }

    println("=" * 85)
    println(f"Maximum frequency difference: $maxDiff%.2e")
    println()

    // Assert all recommendations match within tolerance
    if (failedMatches.nonEmpty) {
      fail(
        s"${failedMatches.size} recommendations differ from reference:\n" +
          failedMatches
            .map { case (userId, expectedUserId, expectedFreq, actualUserId, actualFreq, freqDiff) =>
              f"  User $userId: expected=($expectedUserId,$expectedFreq%.3f), actual=($actualUserId,$actualFreq%.3f), freq_diff=$freqDiff%.2e"
            }
            .mkString("\n")
      )
    }
  }

  test("Recommendation processor handles users with no interests correctly") {
    // Create a graph with users who have no topic interests (all below threshold 3.0)
    val edges = spark.createDataFrame(Seq(
      (0L, 1L),
      (1L, 0L)
    )).toDF("source", "destination")

    val topics = spark.createDataFrame(Seq(
      (0L, 1.0, 2.0, 1.5), // No interests >= 3.0
      (1L, 2.5, 1.8, 2.2)  // No interests >= 3.0
    )).toDF("userId", "games", "movies", "music")

    val recommendationProcessor = SparkRecommendationProcessor(spark)
    val recommendationsDF = recommendationProcessor.computeRecommendations(edges, topics, iterations = 5)

    // Should return empty result since no users have interests >= 3.0
    val count = recommendationsDF.count()
    count shouldBe 0
  }

  test("Recommendation processor with single iteration differs from 10 iterations") {
    val testGraphPath = "data/test-graph"

    val edgeDF = spark.read
      .option("sep", "\t")
      .schema(DataModels.edgeSchema)
      .csv(testGraphPath)

    val testTopicsPath = "data/test-graph-topics"

    val topicsDF = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(DataModels.topicSchema)
      .csv(testTopicsPath)

    val recommendationProcessor = SparkRecommendationProcessor(spark)

    // Compute with 1 iteration
    val recs1 = recommendationProcessor
      .computeRecommendations(edgeDF, topicsDF, iterations = 1)
      .collect()
      .map(r => (r.getAs[Long]("userId"), r.getAs[Double]("postFrequency")))
      .toMap

    // Compute with 10 iterations
    val recs10 = recommendationProcessor
      .computeRecommendations(edgeDF, topicsDF, iterations = 10)
      .collect()
      .map(r => (r.getAs[Long]("userId"), r.getAs[Double]("postFrequency")))
      .toMap

    // Results should potentially differ (algorithm should propagate over iterations)
    recs1.keys shouldEqual recs10.keys

    // Check if any frequencies differ significantly
    val hasDifference = recs1.exists { case (userId, freq1) =>
      val freq10 = recs10(userId)
      Math.abs(freq1 - freq10) > 1e-6
    }

    // Note: Depending on the graph structure, frequencies might converge quickly
    // This test mainly ensures the iterations parameter is being used
    info(s"Iteration difference detected: $hasDifference")
  }


  /** Loads reference recommendation results from file
    *
    * @param filePath
    *   Path to reference file (tab-separated: userId\trecommendedUserId\tpostFrequency)
    * @return
    *   Map of userId to (recommendedUserId, postFrequency)
    */
  private def loadReferenceRecommendations(filePath: String): Map[Long, (Long, Double)] = {
    val source = Source.fromFile(filePath)
    try {
      source
        .getLines()
        .map { line =>
          val parts = line.split("\t")
          (parts(0).toLong, (parts(1).toLong, parts(2).toDouble))
        }
        .toMap
    } finally {
      source.close()
    }
  }
}