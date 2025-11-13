package com.twitter.analysis.interfaces

import org.apache.spark.sql.DataFrame

/** Interface for formatting, displaying, and persisting analysis results to S3 Includes
  * visualization capabilities for Databricks notebooks
  */
trait ResultsOutput {

  /** Displays graph statistics in notebook-friendly format
    * @param edgeCount
    *   Total number of edges in the graph
    * @param vertexCount
    *   Total number of vertices in the graph
    */
  def displayGraphStats(edgeCount: Long, vertexCount: Long): Unit

  /** Displays top users with their follower counts in formatted output
    * @param topUsersDF
    *   DataFrame containing popular users
    */
  def displayTopUsers(topUsersDF: DataFrame): Unit

  /** Persists analysis results to S3 bucket in structured format
    * @param results
    *   DataFrame containing results to save
    * @param s3OutputPath
    *   S3 path for saving results
    */
  def saveResultsToS3SingleFile(results: DataFrame, s3OutputPath: String): Unit
}
