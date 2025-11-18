package com.twitter.analysis.interfaces

import org.apache.spark.sql.types.StructType
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

  /** Loads topics data containing user posting frequencies
    * @param inputPath
    *   The input path to the topics data file (tab-separated: userID\tgames\tmovies\tmusic)
    * @return
    *   DataFrame containing userId, games, movies, music frequencies
    */
  def loadTopicsData(inputPath: String): DataFrame

  /** Validate the input path format
    * @param inputPath
    *   The input path to the edge list file
    */
  protected def validatePath(inputPath: String): Unit =
    if (
      !inputPath.startsWith("s3://") && !inputPath
        .startsWith("s3a://") && !inputPath.startsWith("s3n://")
    ) {
      throw new IllegalArgumentException(
        s"Invalid S3 path format: $inputPath. Must start with s3://, s3a://, or s3n://"
      )
    }

  /** Reads data from the specified input path with given schema
    * @param spark
    *   The spark session
    * @param inputPath
    *   The input path to read data from
    * @param schema
    *   The schema to apply when reading the data
    * @return
    *   DataFrame containing the read data
    */
  protected def readData(
      spark: SparkSession,
      inputPath: String,
      schema: StructType,
      hasHeader: Boolean = false
  ): DataFrame =
    spark.read
      .option("sep", "\t")
      .option("header", hasHeader.toString)
      .option("multiline", "false")
      .option("escape", "")
      .option("quote", "")
      .option("inferSchema", "false")
      .schema(schema)
      .csv(inputPath)

  /** Reads data with optimizations for large datasets
    * @param spark
    *   The spark session
    * @param inputPath
    *   The input path to optimize reading for
    * @return
    *   Optimized DataFrame
    */
  protected def readDataOptimized(
      spark: SparkSession,
      inputPath: String,
      schema: StructType,
      hasHeader: Boolean = false
  ): DataFrame = {
    val df                = readData(spark, inputPath, schema, hasHeader)
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
