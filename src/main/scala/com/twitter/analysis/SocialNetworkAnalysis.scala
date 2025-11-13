package com.twitter.analysis

import com.twitter.analysis.impl.{S3DataLoader, S3ResultsOutput, SparkGraphProcessor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SocialNetworkAnalysis {

  private val APP_NAME = "TwitterSocialNetworkAnalysis"

  def main(args: Array[String]): Unit = {
    // Validate command-line arguments
    if (args.length < 2) {
      println("Usage: SocialNetworkAnalysis <input-path> <output-path> [topN]")
      System.exit(1)
    }
    val inputPath  = args(0)
    val outputPath = args(1)
    val topN       = if (args.length >= 3) args(2).toInt else 100

    // Initialize Spark session and components
    val spark          = SparkSession.builder().appName(APP_NAME).getOrCreate()
    val dataLoader     = S3DataLoader.apply(spark)
    val graphProcessor = SparkGraphProcessor.apply(spark)
    val resultsOutput  = S3ResultsOutput.apply(spark)

    // Main analysis workflow
    try {
      val df = dataLoader.loadEdgeList(inputPath)
      df.persist(StorageLevel.MEMORY_AND_DISK_SER)

      try {
        val edgeCount = graphProcessor.countEdges(df)

        val allUsers = graphProcessor.getAllUsers(df)
        allUsers.persist(StorageLevel.MEMORY_AND_DISK_SER)

        try {
          val vertexCount = graphProcessor.countVertices(allUsers)

          resultsOutput.displayGraphStats(edgeCount, vertexCount)

          val statsDF = graphProcessor.createGraphStatsDF(edgeCount, vertexCount)
          resultsOutput.saveResultsToS3SingleFile(statsDF, s"$outputPath/graph-stats")

          val topNUsers = graphProcessor.getTopNUsers(df, allUsers, topN)
          resultsOutput.saveResultsToS3SingleFile(topNUsers, s"$outputPath/top-$topN-users")
          resultsOutput.displayTopUsers(topNUsers)
        } finally
          allUsers.unpersist()
      } finally
        df.unpersist()
    } finally
      spark.stop()
  }
}
