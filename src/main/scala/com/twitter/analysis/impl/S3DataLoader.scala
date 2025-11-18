package com.twitter.analysis.impl

import com.twitter.analysis.config.AWSConfig
import com.twitter.analysis.interfaces.DataLoader
import com.twitter.analysis.models.DataModels
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** S3-enabled implementation of DataLoader for reading Twitter social graph data from Amazon S3
  * storage with optimizations for large datasets (10+ GB)
  */
class S3DataLoader(spark: SparkSession) extends DataLoader {

  override def loadEdgeList(inputPath: String): DataFrame = {
    validatePath(inputPath)

    try {
      val optimizedDF = readDataOptimized(spark, inputPath, DataModels.edgeSchema)
      filterInvalidEdges(optimizedDF)

    } catch {
      case e: org.apache.hadoop.fs.s3a.AWSBadRequestException =>
        throw new RuntimeException(
          s"S3 access denied or bucket not found: $inputPath. Check IAM permissions.",
          e
        )
      case e: java.io.FileNotFoundException                   =>
        throw new RuntimeException(s"S3 object not found: $inputPath. Verify the path exists.", e)
      case e: Exception                                       =>
        throw new RuntimeException(s"Failed to load edge list from S3: $inputPath", e)
    }
  }

  override def loadTopicsData(inputPath: String): DataFrame = {
    validatePath(inputPath)

    try {
      val df = readData(spark, inputPath, DataModels.topicSchema)
      filterInvalidTopics(df)

    } catch {
      case e: org.apache.hadoop.fs.s3a.AWSBadRequestException =>
        throw new RuntimeException(
          s"S3 access denied or bucket not found: $inputPath. Check IAM permissions.",
          e
        )
      case e: java.io.FileNotFoundException                   =>
        throw new RuntimeException(
          s"S3 topics data not found: $inputPath. Verify the path exists.",
          e
        )
      case e: Exception                                       =>
        throw new RuntimeException(s"Failed to load topics data from S3: $inputPath", e)
    }
  }

  /** Configures S3 access credentials and settings for the Spark session Sets up IAM role
    * authentication and S3-specific optimizations
    */
  private def configureS3Access(): Unit = {
    AWSConfig.configureS3Access(spark)

    // Additional S3 configurations for large file processing
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Enable S3A committer for better performance
    hadoopConf.set(
      "mapreduce.outputcommitter.factory.scheme.s3a",
      "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
    )

    // Set retry and timeout configurations
    hadoopConf.set("fs.s3a.retry.limit", "10")
    hadoopConf.set("fs.s3a.retry.interval", "500ms")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "true")
  }

  /** Validates that edge data contains exactly two user IDs per line
    * @param df
    *   The DataFrame to validate
    * @return
    *   Validated DataFrame with proper edge format
    */
  private def filterInvalidEdges(df: DataFrame): DataFrame =
    df.select(
      col("source").cast(LongType).as("source"),
      col("destination").cast(LongType).as("destination")
    ).filter(col("source").isNotNull && col("destination").isNotNull)

  /** Validates that topics data contains valid user ID and frequency values
    * @param df
    *   The DataFrame to validate
    * @return
    *   Validated DataFrame with proper topics format
    */
  private def filterInvalidTopics(df: DataFrame): DataFrame =
    df.select(
      col("userId").cast(LongType).as("userId"),
      col("games").cast(DoubleType).as("games"),
      col("movies").cast(DoubleType).as("movies"),
      col("music").cast(DoubleType).as("music")
    ).filter(
      col("userId").isNotNull &&
        col("games").isNotNull && col("games") >= 0.0 &&
        col("movies").isNotNull && col("movies") >= 0.0 &&
        col("music").isNotNull && col("music") >= 0.0
    )
}

/** Companion object for S3DataLoader with factory methods
  */
object S3DataLoader {

  /** Creates a new S3DataLoader instance with the given SparkSession
    * @param spark
    *   The SparkSession to use
    * @return
    *   New S3DataLoader instance
    */
  def apply(spark: SparkSession): S3DataLoader = {
    val loader = new S3DataLoader(spark)
    loader.configureS3Access()
    loader
  }
}
