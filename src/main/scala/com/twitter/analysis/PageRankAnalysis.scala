package com.twitter.analysis

import com.twitter.analysis.impl.{S3DataLoader, S3ResultsOutput, SparkPageRankProcessor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/** Driver application for computing PageRank on Twitter social network graph
  *
  * This application analyzes user influence in a Twitter social network using the PageRank
  * algorithm. It identifies the most influential users based on their follower network structure.
  *
  * Usage: PageRankAnalysis <input-path> <output-path> [iterations] [topN]
  *
  * Arguments:
  *   - input-path: S3 path to the edge list file (tab-separated: source\tdestination)
  *   - output-path: S3 path for output results
  *   - iterations: Number of PageRank iterations (default: 10)
  *   - topN: Number of top ranked users to display (default: 100)
  *
  * Output:
  *   - pagerank-results/: Complete PageRank scores stored in Parquet format in S3
  *   - top-N-ranked-users/: Top N users by PageRank score stored in Parquet format in S3
  */
object PageRankAnalysis {

  private val APP_NAME           = "TwitterPageRankAnalysis"
  private val DEFAULT_ITERATIONS = 10
  private val DEFAULT_TOP_N      = 100
  private val DAMPING_FACTOR     = 0.85

  def main(args: Array[String]): Unit = {
    // Validate command-line arguments
    if (args.length < 2) {
      println("Usage: PageRankAnalysis <input-path> <output-path> [iterations] [topN]")
      println("  input-path  : S3 path to edge list (tab-separated: source\\tdestination)")
      println("  output-path : S3 path for output results")
      println("  iterations  : Number of PageRank iterations (default: 10)")
      println("  topN        : Number of top users to display (default: 100)")
      System.exit(1)
    }

    val inputPath  = args(0)
    val outputPath = args(1)
    val iterations = if (args.length >= 3) args(2).toInt else DEFAULT_ITERATIONS
    val topN       = if (args.length >= 4) args(3).toInt else DEFAULT_TOP_N

    // Initialize Spark session and components
    val spark             = SparkSession.builder().appName(APP_NAME).getOrCreate()
    val dataLoader        = S3DataLoader.apply(spark)
    val pageRankProcessor = SparkPageRankProcessor.apply(spark)
    val resultsOutput     = S3ResultsOutput.apply(spark)

    // Main PageRank computation workflow
    try {
      val startTime = System.currentTimeMillis()

      // Load edge list from S3
      val edgeDF = dataLoader.loadEdgeList(inputPath)
      edgeDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      edgeDF.count() // Trigger action to materialize the DataFrame

      try {
        // Compute PageRank
        val pageRankDF = pageRankProcessor.computePageRank(
          edgeDF,
          iterations = iterations,
          dampingFactor = DAMPING_FACTOR
        )

        // Cache results for multiple outputs
        pageRankDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

        try {
          // Save complete PageRank results
          resultsOutput.saveResultsToS3(pageRankDF, s"$outputPath/pagerank-results")

          // Get and display top N ranked users
          val topNRanked = pageRankProcessor.getTopNRankedUsers(pageRankDF, topN)
          resultsOutput.displayTopRankedUsers(topNRanked)

          // Save top N results in Parquet format for easy querying
          resultsOutput.saveResultsToS3SingleFile(topNRanked, s"$outputPath/top-$topN-ranked-users")

          val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
          println("=" * 80)
          println(f"PageRank analysis completed successfully in $totalTime%.2f seconds")
          println("=" * 80)

        } finally
          pageRankDF.unpersist()
      } finally
        edgeDF.unpersist()

    } catch {
      case e: Exception =>
        println(s"Error during PageRank analysis: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally
      spark.stop()
  }
}
