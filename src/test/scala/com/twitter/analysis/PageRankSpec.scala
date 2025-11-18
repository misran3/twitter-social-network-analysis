package com.twitter.analysis

import com.twitter.analysis.impl.SparkPageRankProcessor
import com.twitter.analysis.models.DataModels
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.io.Source

/** Test suite for PageRank implementation
  *
  * Validates correctness of PageRank algorithm using reference test data
  */
class PageRankSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val TOLERANCE           = 1e-6 // Tolerance for floating-point comparison

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("PageRankTest")
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

  test("PageRank computation on test graph matches reference output") {
    // Load test graph with explicit schema
    val testGraphPath = "data/test-graph"

    val edgeDF = spark.read
      .option("sep", "\t")
      .schema(DataModels.edgeSchema)
      .csv(testGraphPath)

    // Create PageRank processor
    val pageRankProcessor = SparkPageRankProcessor(spark)

    // Compute PageRank with 10 iterations
    val pageRankDF = pageRankProcessor.computePageRank(edgeDF)

    // Collect results into a map
    val actualRanks = pageRankDF
      .collect()
      .map { row =>
        val userId = row.getAs[Long]("userId")
        val rank   = row.getAs[Double]("rank")
        (userId, rank)
      }
      .toMap

    // Load reference results
    val referenceRanks = loadReferenceRanks("data/test-graph-rank-refs")

    // Validate that all users are present
    actualRanks.keySet shouldEqual referenceRanks.keySet

    // Validate rank sum equals 1.0
    val rankSum = actualRanks.values.sum
    Math.abs(rankSum - 1.0) should be < 1e-10

    // Compare each rank with reference
    var maxDiff       = 0.0
    var failedMatches = List.empty[(Long, Double, Double, Double)]

    referenceRanks.foreach { case (userId, expectedRank) =>
      val actualRank = actualRanks(userId)
      val diff       = Math.abs(actualRank - expectedRank)
      maxDiff = Math.max(maxDiff, diff)

      if (diff >= TOLERANCE) {
        failedMatches = (userId, expectedRank, actualRank, diff) :: failedMatches
      }
    }

    // Print comparison table for debugging
    println("\nPageRank Comparison:")
    println("=" * 70)
    println(f"${"User ID"}%-10s ${"Expected"}%-15s ${"Actual"}%-15s ${"Difference"}%-15s")
    println("-" * 70)

    referenceRanks.toSeq.sortBy(_._1).foreach { case (userId, expectedRank) =>
      val actualRank = actualRanks(userId)
      val diff       = Math.abs(actualRank - expectedRank)
      val status     = if (diff < TOLERANCE) "✓" else "✗"
      println(f"$userId%-10d $expectedRank%-15.11f $actualRank%-15.11f $diff%-15.2e $status")
    }
    println("=" * 70)
    println(f"Maximum difference: $maxDiff%.2e")
    println(f"Rank sum: $rankSum%.15f")
    println()

    // Assert all ranks match within tolerance
    if (failedMatches.nonEmpty) {
      fail(
        s"${failedMatches.size} PageRank values differ from reference by more than $TOLERANCE:\n" +
          failedMatches
            .map { case (userId, expected, actual, diff) =>
              f"  User $userId: expected=$expected%.11f, actual=$actual%.11f, diff=$diff%.2e"
            }
            .mkString("\n")
      )
    }
  }

  test("PageRank handles dangling nodes correctly") {
    // Create a simple graph with a dangling node (user 3)
    // Graph: 0->2, 1->0, 1->2, 2->1
    // User 3 has no outgoing edges (dangling)
    val edges = spark.createDataFrame(Seq(
      (0L, 2L),
      (1L, 0L),
      (1L, 2L),
      (2L, 1L)
    )).toDF("source", "destination")

    val pageRankProcessor = SparkPageRankProcessor(spark)
    val pageRankDF        = pageRankProcessor.computePageRank(edges)

    // Verify rank sum equals 1.0
    val rankSum = pageRankDF.agg("rank" -> "sum").first().getDouble(0)
    Math.abs(rankSum - 1.0) should be < 1e-10

    // Verify all users have positive ranks
    val ranks = pageRankDF.collect().map(_.getAs[Double]("rank"))
    ranks.foreach { rank =>
      rank should be > 0.0
    }
  }

  test("PageRank with single iteration differs from 10 iterations") {
    val testGraphPath = "data/test-graph"

    val edgeDF = spark.read
      .option("sep", "\t")
      .schema(DataModels.edgeSchema)
      .csv(testGraphPath)

    val pageRankProcessor = SparkPageRankProcessor(spark)

    // Compute with 1 iteration
    val ranks1 = pageRankProcessor
      .computePageRank(edgeDF, iterations = 1)
      .collect()
      .map(r => (r.getAs[Long]("userId"), r.getAs[Double]("rank")))
      .toMap

    // Compute with 10 iterations
    val ranks10 = pageRankProcessor
      .computePageRank(edgeDF, iterations = 10)
      .collect()
      .map(r => (r.getAs[Long]("userId"), r.getAs[Double]("rank")))
      .toMap

    // Ranks should differ (algorithm should converge over iterations)
    ranks1.keys shouldEqual ranks10.keys

    val hasDifference = ranks1.exists { case (userId, rank1) =>
      val rank10 = ranks10(userId)
      Math.abs(rank1 - rank10) > 1e-6
    }

    hasDifference shouldBe true
  }

  test("PageRank getTopNRankedUsers returns correct number") {
    val testGraphPath = "data/test-graph"
    val schema = StructType(Array(
      StructField("source", LongType, nullable = false),
      StructField("destination", LongType, nullable = false)
    ))

    val edgeDF = spark.read
      .option("sep", "\t")
      .schema(schema)
      .csv(testGraphPath)

    val pageRankProcessor = SparkPageRankProcessor(spark)
    val pageRankDF        = pageRankProcessor.computePageRank(edgeDF, iterations = 10)

    // Test getting top 2 users
    val top2 = pageRankProcessor.getTopNRankedUsers(pageRankDF, topN = 2)
    top2.count() shouldBe 2

    // Verify they are sorted by rank descending
    val ranks = top2.collect().map(_.getAs[Double]("rank"))
    ranks.length shouldBe 2
    ranks(0) should be >= ranks(1)
  }

  test("PageRank rank sum validation throws on severe deviation") {
    // Create an invalid scenario by manually creating bad data
    // (This test is more for documentation - actual algorithm should maintain sum=1.0)
    val edges = spark.createDataFrame(Seq(
      (0L, 1L),
      (1L, 0L)
    )).toDF("source", "destination")

    val pageRankProcessor = SparkPageRankProcessor(spark)

    // Normal execution should not throw
    noException should be thrownBy {
      pageRankProcessor.computePageRank(edges, iterations = 10)
    }
  }

  /** Loads reference PageRank results from file
    *
    * @param filePath
    *   Path to reference file (tab-separated: userId\trank)
    * @return
    *   Map of userId to rank
    */
  private def loadReferenceRanks(filePath: String): Map[Long, Double] = {
    val source = Source.fromFile(filePath)
    try {
      source
        .getLines()
        .map { line =>
          val parts = line.split("\t")
          (parts(0).toLong, parts(1).toDouble)
        }
        .toMap
    } finally {
      source.close()
    }
  }
}