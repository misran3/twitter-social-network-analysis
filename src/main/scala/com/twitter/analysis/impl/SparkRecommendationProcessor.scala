package com.twitter.analysis.impl

import com.twitter.analysis.interfaces.RecommendationProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/** Spark-based implementation of RecommendationProcessor that computes follower recommendations
  * based on topic interests.
  *
  * This implementation uses iterative topic frequency propagation through the social graph.
  */
class SparkRecommendationProcessor(spark: SparkSession) extends RecommendationProcessor {

  private val logger             = org.apache.log4j.Logger.getLogger(getClass.getName)
  private val INTEREST_THRESHOLD = 3.0

  override def computeRecommendations(
      edgeDF: DataFrame,
      topicsDF: DataFrame,
      iterations: Int = 10
  ): DataFrame = {

    logger.info(
      s"Starting follower recommendations computation with $iterations iterations"
    )

    // Step 1: Get all unique users and prepare graph structure
    val graphProcessor = SparkGraphProcessor.apply(spark)
    val allUsers       = graphProcessor.getAllUsers(edgeDF)

    // Step 2: Build adjacency list with out-degrees
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

    // Step 3: Prepare topics data with interests flags
    val topicsWithInterests = topicsDF
      .withColumn("interestedInGames", col("games") >= INTEREST_THRESHOLD)
      .withColumn("interestedInMovies", col("movies") >= INTEREST_THRESHOLD)
      .withColumn("interestedInMusic", col("music") >= INTEREST_THRESHOLD)

    // Step 4: Initialize state - combine all users with topics data
    var state = allUsers
      .join(topicsWithInterests, allUsers("userId") === topicsWithInterests("userId"), "left_outer")
      .select(
        allUsers("userId"),
        // Topics data (fill with 0.0 if missing)
        coalesce(topicsWithInterests("games"), lit(0.0)).as("games"),
        coalesce(topicsWithInterests("movies"), lit(0.0)).as("movies"),
        coalesce(topicsWithInterests("music"), lit(0.0)).as("music"),
        // Interest flags (fill with false if missing)
        coalesce(topicsWithInterests("interestedInGames"), lit(false)).as("interestedInGames"),
        coalesce(topicsWithInterests("interestedInMovies"), lit(false)).as("interestedInMovies"),
        coalesce(topicsWithInterests("interestedInMusic"), lit(false)).as("interestedInMusic"),
        // Max frequencies (initially user's own frequencies)
        coalesce(topicsWithInterests("games"), lit(0.0)).as("maxGamesFreq"),
        coalesce(topicsWithInterests("movies"), lit(0.0)).as("maxMoviesFreq"),
        coalesce(topicsWithInterests("music"), lit(0.0)).as("maxMusicFreq"),
        // Originating users (initially user themselves)
        allUsers("userId").as("maxGamesUser"),
        allUsers("userId").as("maxMoviesUser"),
        allUsers("userId").as("maxMusicUser")
      )

    // Step 5: Iterative computation
    for (i <- 1 to iterations) {
      val iterationStart = System.currentTimeMillis()

      state = computeIteration(state, followingList)

      val iterationTime = System.currentTimeMillis() - iterationStart
      logger.info(f"Iteration $i completed in ${iterationTime}ms")
    }

    // Step 6: Generate final recommendations
    val recommendationsDF = generateRecommendations(state)

    // Cleanup cached DataFrames
    followingList.unpersist()

    logger.info("Follower recommendations computation completed")
    recommendationsDF
  }

  /** Computes one iteration of the recommendation algorithm */
  private def computeIteration(
      state: DataFrame,
      followingList: DataFrame
  ): DataFrame = {

    // Join state with following lists
    val stateWithFollowing = state
      .join(followingList, Seq("userId"), "left_outer")

    // Create contributions for followees (only topic frequencies)
    val contributions = stateWithFollowing
      .withColumn("followee", explode_outer(col("followees")))
      .withColumn(
        "gamesFreqContrib",
        when(col("followees").isNotNull && col("interestedInGames"), col("maxGamesFreq"))
          .otherwise(0.0)
      )
      .withColumn(
        "moviesFreqContrib",
        when(col("followees").isNotNull && col("interestedInMovies"), col("maxMoviesFreq"))
          .otherwise(0.0)
      )
      .withColumn(
        "musicFreqContrib",
        when(col("followees").isNotNull && col("interestedInMusic"), col("maxMusicFreq"))
          .otherwise(0.0)
      )
      .withColumn(
        "gamesUserContrib",
        when(col("followees").isNotNull && col("interestedInGames"), col("maxGamesUser"))
          .otherwise(lit(-1L)) // Use -1 to indicate no contribution
      )
      .withColumn(
        "moviesUserContrib",
        when(col("followees").isNotNull && col("interestedInMovies"), col("maxMoviesUser"))
          .otherwise(lit(-1L))
      )
      .withColumn(
        "musicUserContrib",
        when(col("followees").isNotNull && col("interestedInMusic"), col("maxMusicUser"))
          .otherwise(lit(-1L))
      )
      .select(
        coalesce(col("followee"), col("userId")).as("receiverId"),
        col("gamesFreqContrib"),
        col("gamesUserContrib"),
        col("moviesFreqContrib"),
        col("moviesUserContrib"),
        col("musicFreqContrib"),
        col("musicUserContrib")
      )

    // Aggregate contributions by recipient
    val aggregatedContribs = contributions
      .groupBy("receiverId")
      .agg(
        max(struct(col("gamesFreqContrib"), col("gamesUserContrib"))).as("maxGamesStruct"),
        max(struct(col("moviesFreqContrib"), col("moviesUserContrib"))).as("maxMoviesStruct"),
        max(struct(col("musicFreqContrib"), col("musicUserContrib"))).as("maxMusicStruct")
      )
      .select(
        col("receiverId").as("userId"),
        col("maxGamesStruct.gamesFreqContrib").as("receivedMaxGamesFreq"),
        col("maxGamesStruct.gamesUserContrib").as("receivedMaxGamesUser"),
        col("maxMoviesStruct.moviesFreqContrib").as("receivedMaxMoviesFreq"),
        col("maxMoviesStruct.moviesUserContrib").as("receivedMaxMoviesUser"),
        col("maxMusicStruct.musicFreqContrib").as("receivedMaxMusicFreq"),
        col("maxMusicStruct.musicUserContrib").as("receivedMaxMusicUser")
      )

    // Update state with new max frequency values
    val newState = state
      .join(aggregatedContribs, Seq("userId"), "left_outer")
      .withColumn(
        "newMaxGamesFreq",
        greatest(
          col("maxGamesFreq"),
          coalesce(col("receivedMaxGamesFreq"), lit(0.0))
        )
      )
      .withColumn(
        "newMaxMoviesFreq",
        greatest(
          col("maxMoviesFreq"),
          coalesce(col("receivedMaxMoviesFreq"), lit(0.0))
        )
      )
      .withColumn(
        "newMaxMusicFreq",
        greatest(
          col("maxMusicFreq"),
          coalesce(col("receivedMaxMusicFreq"), lit(0.0))
        )
      )
      .withColumn(
        "newMaxGamesUser",
        when(
          coalesce(col("receivedMaxGamesFreq"), lit(0.0)) > col("maxGamesFreq"),
          coalesce(col("receivedMaxGamesUser"), col("maxGamesUser"))
        ).otherwise(col("maxGamesUser"))
      )
      .withColumn(
        "newMaxMoviesUser",
        when(
          coalesce(col("receivedMaxMoviesFreq"), lit(0.0)) > col("maxMoviesFreq"),
          coalesce(col("receivedMaxMoviesUser"), col("maxMoviesUser"))
        ).otherwise(col("maxMoviesUser"))
      )
      .withColumn(
        "newMaxMusicUser",
        when(
          coalesce(col("receivedMaxMusicFreq"), lit(0.0)) > col("maxMusicFreq"),
          coalesce(col("receivedMaxMusicUser"), col("maxMusicUser"))
        ).otherwise(col("maxMusicUser"))
      )
      .select(
        col("userId"),
        col("games"),
        col("movies"),
        col("music"),
        col("interestedInGames"),
        col("interestedInMovies"),
        col("interestedInMusic"),
        col("newMaxGamesFreq").as("maxGamesFreq"),
        col("newMaxMoviesFreq").as("maxMoviesFreq"),
        col("newMaxMusicFreq").as("maxMusicFreq"),
        col("newMaxGamesUser").as("maxGamesUser"),
        col("newMaxMoviesUser").as("maxMoviesUser"),
        col("newMaxMusicUser").as("maxMusicUser")
      )

    newState
  }

  /**
   * Generates final recommendations based on the propagated topic frequencies
   * @param finalState DataFrame containing the final state after iterations
   * @return DataFrame with recommendations for each user
   */
  private def generateRecommendations(finalState: DataFrame): DataFrame =
    // For each user, find the topic with the highest frequency among their interests
    // Tie-breaking: higher user ID wins (as per requirements)
    finalState
      .withColumn(
        "gamesCandidate",
        when(
          col("interestedInGames"),
          struct(
            col("maxGamesFreq").as("freq"),
            (-col("maxGamesUser")).as("negUser"),
            col("maxGamesUser").as("user"),
            lit("games").as("topic")
          )
        ).otherwise(
          struct(
            lit(-1.0).as("freq"),
            lit(1L).as("negUser"),
            lit(-1L).as("user"),
            lit("none").as("topic")
          )
        )
      )
      .withColumn(
        "moviesCandidate",
        when(
          col("interestedInMovies"),
          struct(
            col("maxMoviesFreq").as("freq"),
            (-col("maxMoviesUser")).as("negUser"),
            col("maxMoviesUser").as("user"),
            lit("movies").as("topic")
          )
        ).otherwise(
          struct(
            lit(-1.0).as("freq"),
            lit(1L).as("negUser"),
            lit(-1L).as("user"),
            lit("none").as("topic")
          )
        )
      )
      .withColumn(
        "musicCandidate",
        when(
          col("interestedInMusic"),
          struct(
            col("maxMusicFreq").as("freq"),
            (-col("maxMusicUser")).as("negUser"),
            col("maxMusicUser").as("user"),
            lit("music").as("topic")
          )
        ).otherwise(
          struct(
            lit(-1.0).as("freq"),
            lit(1L).as("negUser"),
            lit(-1L).as("user"),
            lit("none").as("topic")
          )
        )
      )
      .withColumn(
        "bestCandidate",
        greatest(col("gamesCandidate"), col("moviesCandidate"), col("musicCandidate"))
      )
      .select(
        col("userId"),
        col("bestCandidate.freq").as("postFrequency"),
        col("bestCandidate.topic").as("topic"),
        col("bestCandidate.user").as("recommendedUserId")
      )
      .filter(col("postFrequency") > 0.0) // Filter out users with no interests
}

/** Companion object for SparkSocialNetworkProcessor with factory methods */
object SparkRecommendationProcessor {

  /** Creates a new SparkSocialNetworkProcessor instance
    *
    * @param spark
    *   SparkSession to use for DataFrame operations
    * @return
    *   New SparkSocialNetworkProcessor instance
    */
  def apply(spark: SparkSession): SparkRecommendationProcessor =
    new SparkRecommendationProcessor(spark)
}
