package com.twitter.analysis.impl

import com.twitter.analysis.interfaces.GraphProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Spark-based implementation of GraphProcessor for core graph operations
  *
  * This class provides efficient implementations of graph counting operations using Spark DataFrame
  * APIs optimized for large-scale distributed processing.
  */
class SparkGraphProcessor(spark: SparkSession) extends GraphProcessor {

  import spark.implicits._

  override def countEdges(edgeDF: DataFrame): Long =
    edgeDF.count()

  override def getAllUsers(edgeDF: DataFrame): DataFrame = {
    val sourceUsers      = edgeDF.select($"source".as("userId"))
    val destinationUsers = edgeDF.select($"destination".as("userId"))
    val unionDF          = sourceUsers.union(destinationUsers)
    unionDF.distinct()
  }

  override def countVertices(allUsers: DataFrame): Long =
    allUsers.count()

  override def createGraphStatsDF(edgeCount: Long, vertexCount: Long): DataFrame =
    Seq(
      ("edge_count", edgeCount),
      ("vertex_count", vertexCount)
    ).toDF("metric", "value")

  override def getTopNUsers(edgeDF: DataFrame, allUsers: DataFrame, topN: Int): DataFrame = {
    val followerDF = calculateFollowerCounts(edgeDF, allUsers)
    followerDF.orderBy(desc("followerCount")).limit(topN)
  }

  /** Calculates follower counts for each user by aggregating incoming edges.
    *
    * This method handles users with zero followers by ensuring all users who appear as sources
    * (following others) are included in the result, even if they have no followers.
    *
    * @param edgeDF
    *   DataFrame containing edge data with 'source' and 'destination' columns
    * @return
    *   DataFrame with 'userId' and 'followerCount' columns for all users
    */
  private def calculateFollowerCounts(edgeDF: DataFrame, allUsers: DataFrame): DataFrame = {
    val followerCounts =
      edgeDF.groupBy($"destination".as("userId")).agg(count("*").as("followerCount"))

    allUsers
      .join(followerCounts, Seq("userId"), "left_outer")
      .select($"userId", coalesce($"followerCount", lit(0L)).as("followerCount"))
  }
}

/** Companion object for SparkGraphProcessor with factory methods and utilities
  */
object SparkGraphProcessor {

  /** Creates a new SparkGraphProcessor instance with optimized configuration
    *
    * @param spark
    *   SparkSession to use for DataFrame operations
    * @return
    *   New SparkGraphProcessor instance
    */
  def apply(spark: SparkSession): SparkGraphProcessor =
    new SparkGraphProcessor(spark)
}
