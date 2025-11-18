package com.twitter.analysis.impl

import com.twitter.analysis.interfaces.PageRankProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/** Spark-based implementation of PageRankProcessor using DataFrame API
  *
  * This implementation uses DataFrames for iterative PageRank computation with optimizations for:
  *   - Minimal shuffles through strategic caching
  *   - Proper handling of dangling nodes (users with no outgoing edges)
  *   - Validation of rank sum convergence
  *   - Efficient contribution aggregation
  */
class SparkPageRankProcessor(spark: SparkSession) extends PageRankProcessor {

  private val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  override def computePageRank(
      edgeDF: DataFrame,
      iterations: Int = 10,
      dampingFactor: Double = 0.85
  ): DataFrame = {

    logger.info(
      s"Starting PageRank computation with $iterations iterations, damping=$dampingFactor"
    )

    // Step 1: Get all unique users
    val graphProcessor = SparkGraphProcessor.apply(spark)
    val allUsers       = graphProcessor.getAllUsers(edgeDF)
    allUsers.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val numUsers = allUsers.count() // Trigger action to materialize the DataFrame
    logger.info(s"Total users in graph: $numUsers")

    // Step 2: Build adjacency list with out-degrees (source -> list of followees)
    val followingList = edgeDF
      .groupBy("source")
      .agg(
        collect_list("destination").as("followees"),
        count("destination").as("outDegree")
      )
      .select(
        col("source").as("userId"),
        col("followees"),
        col("outDegree")
      )

    followingList.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Step 3: Identify dangling users (users with no outgoing edges)
    val usersWithOutDegree = followingList.select("userId")
    val danglingUsers      = allUsers
      .join(usersWithOutDegree, Seq("userId"), "left_anti")
      .select("userId")

    danglingUsers.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val danglingCount = danglingUsers.count()
    logger.info(s"Dangling users (no outgoing edges): $danglingCount")

    // Step 4: Initialize ranks (1/N for all users)
    val initialRank = 1.0 / numUsers
    var ranks       = allUsers.withColumn("rank", lit(initialRank))

    // Step 5: Iterative PageRank computation
    for (i <- 1 to iterations) {
      val iterationStart = System.currentTimeMillis()

      ranks =
        computeIteration(ranks, followingList, allUsers, danglingUsers, numUsers, dampingFactor)

      // Validate rank sum (optional)
      validateRankSum(ranks, iteration = i)

      val iterationTime = System.currentTimeMillis() - iterationStart
      logger.info(f"Iteration $i completed in ${iterationTime}ms")
    }

    // Cleanup cached DataFrames
    followingList.unpersist()
    danglingUsers.unpersist()
    allUsers.unpersist()

    // Return ranks sorted by rank descending
    ranks.orderBy(desc("rank"))
  }

  /** Performs a single iteration of PageRank computation
    * @param ranks
    *   Current ranks DataFrame
    * @param followingList
    *   Following list DataFrame
    * @param allUsers
    *   DataFrame of all users
    * @param danglingUsers
    *   DataFrame of dangling users
    * @param numUsers
    *   Total number of users
    * @param dampingFactor
    *   Damping factor for PageRank
    * @return
    */
  private def computeIteration(
      ranks: DataFrame,
      followingList: DataFrame,
      allUsers: DataFrame,
      danglingUsers: DataFrame,
      numUsers: Long,
      dampingFactor: Double
  ): DataFrame = {
    // Join ranks with following lists (only for users who follow others)
    val ranksWithFollowing = ranks
      .join(followingList, Seq("userId"), "left_outer")

    // Calculate contributions each user makes to their followees
    val contributions = ranksWithFollowing
      .withColumn("followee", explode_outer(col("followees")))
      .withColumn(
        "contrib",
        when(col("followees").isNotNull, col("rank") / col("outDegree"))
          .otherwise(0.0)
      )
      .select(
        coalesce(col("followee"), col("userId")).as("userId"),
        col("contrib")
      )

    // Aggregate contributions received by each user
    val aggregatedContribs = contributions
      .groupBy("userId")
      .agg(sum("contrib").as("totalContrib"))

    // Calculate dangling mass (sum of ranks of dangling users)
    val danglingMassRow = ranks
      .join(danglingUsers, Seq("userId"), "inner")
      .agg(sum("rank").as("danglingMass"))
      .first()

    val danglingMass = if (danglingMassRow.isNullAt(0)) 0.0 else danglingMassRow.getDouble(0)

    // Distribute dangling mass equally to all users
    val danglingContrib = danglingMass / numUsers

    // Apply PageRank formula: rank = (1-d)/N + d * (contributions + danglingContrib)
    val newRanks = allUsers
      .join(aggregatedContribs, Seq("userId"), "left_outer")
      .withColumn(
        "rank",
        lit((1.0 - dampingFactor) / numUsers) +
          lit(dampingFactor) * (coalesce(col("totalContrib"), lit(0.0)) + lit(danglingContrib))
      )
      .select("userId", "rank")

    newRanks
  }

  /** Validates that the sum of ranks is approximately 1.0
    * @param ranks
    *   Current ranks DataFrame
    * @param iteration
    *   Current iteration number
    */
  private def validateRankSum(ranks: DataFrame, iteration: Int): Unit = {
    val rankSum  = ranks.agg(sum("rank")).first().getDouble(0)
    val rankDiff = Math.abs(rankSum - 1.0)

    if (rankDiff > 1e-10) {
      logger.warn(
        f"Iteration $iteration: Rank sum deviation = $rankDiff%.15f (sum = $rankSum%.15f)"
      )
    }

    if (rankDiff > 1e-6) {
      throw new RuntimeException(
        f"PageRank validation failed at iteration $iteration: sum = $rankSum%.15f (expected 1.0)"
      )
    }
  }

  override def getTopNRankedUsers(pageRankDF: DataFrame, topN: Int = 100): DataFrame =
    pageRankDF.limit(topN)
}

/** Companion object for SparkPageRankProcessor with factory methods
  */
object SparkPageRankProcessor {

  /** Creates a new SparkPageRankProcessor instance
    *
    * @param spark
    *   SparkSession to use for DataFrame operations
    * @return
    *   New SparkPageRankProcessor instance
    */
  def apply(spark: SparkSession): SparkPageRankProcessor =
    new SparkPageRankProcessor(spark)
}
