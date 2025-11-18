package com.twitter.analysis.interfaces

import org.apache.spark.sql.DataFrame

/** Interface for PageRank computation on social graphs
  *
  * PageRank is an algorithm that measures the relative importance of vertices in a graph based on
  * the structure of incoming links. In a social network context, it identifies influential users.
  */
trait PageRankProcessor {

  /** Computes PageRank scores for all users in the social graph
    *
    * The algorithm iteratively calculates influence scores where a user's rank is determined by
    * both the number and quality of their followers. Users followed by influential users receive
    * higher ranks.
    *
    * Implementation requirements:
    *   - Initial rank: 1/N for each user (N = total users)
    *   - Damping factor: typically 0.85
    *   - Must handle dangling nodes (users with no outgoing edges)
    *   - Rank sum must equal 1.0 across all users in each iteration
    *
    * @param edgeDF
    *   DataFrame containing edge data with 'source' and 'destination' columns where (source,
    *   destination) represents "source follows destination"
    * @param iterations
    *   Number of iterations to run (default: 10)
    * @param dampingFactor
    *   Probability of following a link vs random jump (default: 0.85)
    * @return
    *   DataFrame with 'userId' and 'rank' columns, sorted by rank descending
    */
  def computePageRank(
      edgeDF: DataFrame,
      iterations: Int = 10,
      dampingFactor: Double = 0.85
  ): DataFrame

  /** Returns top N users by PageRank score
    *
    * @param pageRankDF
    *   DataFrame containing PageRank results with 'userId' and 'rank' columns
    * @param topN
    *   Number of top users to return (default: 100)
    * @return
    *   DataFrame with top N users sorted by rank descending
    */
  def getTopNRankedUsers(pageRankDF: DataFrame, topN: Int = 100): DataFrame
}
