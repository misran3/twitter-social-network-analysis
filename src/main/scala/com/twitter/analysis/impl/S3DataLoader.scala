package com.twitter.analysis.impl

import com.twitter.analysis.config.AWSConfig
import com.twitter.analysis.interfaces.DataLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

/** S3-enabled implementation of DataLoader for reading Twitter social graph data from Amazon S3
  * storage with optimizations for large datasets (10+ GB)
  */
class S3DataLoader(spark: SparkSession) extends DataLoader {

  /** Loads edge list data from S3 bucket using secure AWS credentials Reads tab-separated files and
    * converts them into a DataFrame with source and destination columns
    *
    * @param inputPath
    *   The S3 path to the edge list file (e.g., s3a://bucket/path/edges.txt)
    * @return
    *   DataFrame containing source and destination user IDs
    */
  def loadEdgeList(inputPath: String): DataFrame = {
    validatePath(inputPath)

    try {
      // Configure S3 access before reading
      configureS3Access()

      // Apply S3 read optimizations and validate format
      val optimizedDF = optimizeReading(spark, inputPath)
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

  /** Validates S3 path format and accessibility
    * @param inputPath
    *   The S3 path to validate
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
  def apply(spark: SparkSession): S3DataLoader =
    new S3DataLoader(spark)
}
