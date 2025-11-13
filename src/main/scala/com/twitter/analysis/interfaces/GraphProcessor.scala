package com.twitter.analysis.interfaces

import org.apache.spark.sql.DataFrame

/** Interface for core graph operations and transformations
  */
trait GraphProcessor {

  /** Counts the total number of directed edges in the social graph
    * @param edgeDF
    *   DataFrame containing edge data
    * @return
    *   Total count of directed edges
    */
  def countEdges(edgeDF: DataFrame): Long

  /** Extracts all unique user IDs from the edge DataFrame
    * @param edgeDF
    *   DataFrame containing edge data
    * @return
    *   DataFrame containing all unique user IDs
    */
  def getAllUsers(edgeDF: DataFrame): DataFrame

  /** Counts the total number of unique vertices in the social graph
    * @param allUsers
    *   DataFrame containing all unique user IDs
    * @return
    *   Total count of unique vertices (users)
    */
  def countVertices(allUsers: DataFrame): Long

  /** Creates a DataFrame summarizing graph statistics
    * @param edgeCount
    *   Total number of edges
    * @param vertexCount
    *   Total number of vertices
    * @return
    *   DataFrame containing graph statistics
    */
  def createGraphStatsDF(edgeCount: Long, vertexCount: Long): DataFrame

  /** Ranks users by follower count and selects top N users
    * @param edgeDF
    *   DataFrame containing edge data
    * @param allUsers
    *   DataFrame containing all unique user IDs
    * @param topN
    *   Number of top users to return (default 100)
    * @return
    *   DataFrame containing top N users sorted by follower count
    */
  def getTopNUsers(edgeDF: DataFrame, allUsers: DataFrame, topN: Int = 100): DataFrame
}
