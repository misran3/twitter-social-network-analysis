package com.twitter.analysis

import com.twitter.analysis.impl.{S3DataLoader, S3ResultsOutput, SparkRecommendationProcessor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/** Driver application for computing follower recommendations on Twitter social network graph
  *
  * This application computes follower recommendations based on topic interests.
  *
  * Usage: UserRecommendationAnalysis <graph-path> <topics-path> <output-path> [iterations]
  *
  * Arguments:
  *   - graph-path: S3 path to the edge list file (tab-separated: source\tdestination)
  *   - topics-path: S3 path to the topics data file (tab-separated: userId\tgames\tmovies\tmusic)
  *   - output-path: S3 path for output results
  *   - iterations: Number of iterations (default: 10)
  *
  * Output:
  *   - recommendations/: Follower recommendations stored in Parquet format in S3
  */
object UserRecommendationAnalysis {

  private val APP_NAME           = "UserRecommendationAnalysis"
  private val DEFAULT_ITERATIONS = 10

  def main(args: Array[String]): Unit = {
    // Validate command-line arguments
    if (args.length < 3) {
      println(
        "Usage: UserRecommendationAnalysis <graph-path> <topics-path> <output-path> [iterations]"
      )
      println("  graph-path  : S3 path to edge list (tab-separated: source\\\\tdestination)")
      println(
        "  topics-path : S3 path to topics data (tab-separated: userId\\\\tgames\\\\tmovies\\\\tmusic)"
      )
      println("  output-path : S3 path for output results")
      println("  iterations  : Number of iterations (default: 10)")
      System.exit(1)
    }

    val graphPath  = args(0)
    val topicsPath = args(1)
    val outputPath = args(2)
    val iterations = if (args.length >= 4) args(3).toInt else DEFAULT_ITERATIONS

    // Initialize Spark session and components
    val spark                  = SparkSession.builder().appName(APP_NAME).getOrCreate()
    val dataLoader             = S3DataLoader.apply(spark)
    val socialNetworkProcessor = SparkRecommendationProcessor.apply(spark)
    val resultsOutput          = S3ResultsOutput.apply(spark)

    // Main social network analysis workflow
    try {
      val startTime = System.currentTimeMillis()

      // Load graph edges from S3
      println("Loading graph edges...")
      val edgeDF    = dataLoader.loadEdgeList(graphPath)
      edgeDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val edgeCount = edgeDF.count() // Trigger action to materialize the DataFrame
      println(s"Loaded $edgeCount edges")

      // Load topics data from S3
      println("Loading topics data...")
      val topicsDF = dataLoader.loadTopicsData(topicsPath)

      try {
        // Compute PageRank and recommendations simultaneously
        println(
          s"Starting combined PageRank and recommendations computation with $iterations iterations..."
        )
        val recommendationsDF =
          socialNetworkProcessor.computeRecommendations(
            edgeDF,
            topicsDF,
            iterations = iterations
          )
        resultsOutput.saveResultsToS3(recommendationsDF, s"$outputPath/recommendations")

        val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
        println("==" * 40)
        println(f"Social network analysis completed successfully in $totalTime%.2f seconds")
        println("==" * 40)

      } finally
        edgeDF.unpersist()
    } catch {
      case e: Exception =>
        println(s"Error during social network analysis: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally
      spark.stop()
  }
}
