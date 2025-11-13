package com.twitter.analysis.interfaces

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Interface for loading and validating Twitter social graph data from cloud storage (S3 or Azure)
  */
trait DataLoader {

  /** Loads edge list data
    * @param inputPath
    *   The input path to the edge list file
    * @return
    *   DataFrame containing source and destination user IDs
    */
  def loadEdgeList(inputPath: String): DataFrame

  /** Validate the input path format
    * @param inputPath
    *   The input path to the edge list file
    */
  protected def validatePath(inputPath: String): Unit

  /** Validates that edge data contains exactly two user IDs per line
    * @param df
    *   The DataFrame to validate
    * @return
    *   Validated DataFrame with proper edge format
    */
  protected def filterInvalidEdges(df: DataFrame): DataFrame =
    df.select(
      col("source").cast(LongType).as("source"),
      col("destination").cast(LongType).as("destination")
    ).filter(col("source").isNotNull && col("destination").isNotNull)

  /** Applies read optimizations for large file processing
    * @param spark
    *   The spark session
    * @param inputPath
    *   The input path to optimize reading for
    * @return
    *   Optimized DataFrame
    */
  protected def optimizeReading(spark: SparkSession, inputPath: String): DataFrame = {
    val edgeSchema: StructType = StructType(
      Array(
        StructField("source", LongType, nullable = false),
        StructField("destination", LongType, nullable = false)
      )
    )

    val df = spark.read
      .option("sep", "\t")
      .option("header", "false")
      .option("multiline", "false")
      .option("escape", "")
      .option("quote", "")
      .option("inferSchema", "false")
      .schema(edgeSchema)
      .csv(inputPath)
      .toDF("source", "destination")

    // Apply partitioning strategy for large datasets
    val optimalPartitions = calculateOptimalPartitions(spark, Some(inputPath))
    println(s"Repartitioning DataFrame to $optimalPartitions partitions for optimal performance.")
    df.repartition(optimalPartitions)
  }

  /** Calculates optimal number of partitions based on file size and cluster resources
    * @param spark
    *   The spark session
    * @param inputPath
    *   Optional input path to determine file size
    * @return
    *   Optimal number of partitions
    */
  private def calculateOptimalPartitions(spark: SparkSession, inputPath: Option[String]): Int = {
    val defaultParallelism    = spark.sparkContext.defaultParallelism
    val targetPartitionSizeMB = 128

    inputPath match {
      case Some(path) =>
        try {
          val fs         = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
          val filePath   = new org.apache.hadoop.fs.Path(path)
          val fileSizeMB = fs.getContentSummary(filePath).getLength / (1024 * 1024)

          val calculatedPartitions = (fileSizeMB / targetPartitionSizeMB).toInt

          // Ensure at least 2x cores, cap at reasonable maximum
          math.max(calculatedPartitions, defaultParallelism * 2).min(defaultParallelism * 20)
        } catch {
          case _: Exception =>
            defaultParallelism * 4 // Fallback if file size cannot be determined
        }

      case None =>
        defaultParallelism * 4 // Default for large datasets
    }
  }
}
