package com.twitter.analysis.impl

import com.twitter.analysis.interfaces.ResultsOutput
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Implementation of ResultsOutput for displaying and persisting analysis results
  */
class S3ResultsOutput(spark: SparkSession) extends ResultsOutput {

  def displayGraphStats(edgeCount: Long, vertexCount: Long): Unit = {
    println("=" * 60)
    println("TWITTER SOCIAL NETWORK ANALYSIS - GRAPH STATISTICS")
    println("=" * 60)
    println(f"Total Edges (Directed):     ${edgeCount}%,d")
    println(f"Total Vertices (Users):     ${vertexCount}%,d")
    println("=" * 60)
    println()
  }

  def displayTopUsers(topUsersDF: DataFrame): Unit = {
    println("=" * 80)
    println("TOP USERS BY FOLLOWER COUNT")
    println("=" * 80)
    println(f"${"User ID"}%-15s ${"Follower Count"}%-15s")
    println("-" * 80)

    val topUsers = topUsersDF.collect()
    topUsers.foreach { row =>
      val userId        = row.getAs[Long]("userId")
      val followerCount = row.getAs[Long]("followerCount")
      println(f"${userId}%-15d ${followerCount}%,15d")
    }

    println("=" * 80)
    println(s"Displayed ${topUsers.length} top users")
    println()
  }

  def saveResultsToS3SingleFile(results: DataFrame, s3OutputPath: String): Unit =
    try {
      println(s"Saving results to S3: $s3OutputPath")

      results
        .coalesce(1) // Reduce to single file for smaller result sets
        .write
        .mode("overwrite")
        .option("compression", "snappy") // Use Snappy compression for better performance
        .parquet(s3OutputPath)

      println(s"Successfully saved results to: $s3OutputPath")

    } catch {
      case e: Exception =>
        println(s"Error saving results to S3: ${e.getMessage}")
        throw e
    }
}

/** Companion object for S3ResultsOutput with factory methods
  */
object S3ResultsOutput {

  /** Factory method to create S3ResultsOutput instance
    * @param spark
    *   SparkSession to use for DataFrame operations
    * @return
    *   New S3ResultsOutput instance
    */
  def apply(spark: SparkSession): S3ResultsOutput =
    new S3ResultsOutput(spark)
}
