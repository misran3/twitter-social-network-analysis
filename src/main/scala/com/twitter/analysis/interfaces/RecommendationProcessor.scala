package com.twitter.analysis.interfaces

import org.apache.spark.sql.DataFrame

/** Interface for processing Twitter social network graph with follower recommendations
  */
trait RecommendationProcessor {

  /** Computes follower recommendations based on topic interests
    *
    * @param edgeDF
    *   DataFrame containing graph edges (source, destination)
    * @param topicsDF
    *   DataFrame containing user topic frequencies (userId, games, movies, music)
    * @param iterations
    *   Number of iterations to run (default: 10)
    * @return
    *   DataFrame containing recommendations
    */
  def computeRecommendations(
      edgeDF: DataFrame,
      topicsDF: DataFrame,
      iterations: Int = 10
  ): DataFrame
}
