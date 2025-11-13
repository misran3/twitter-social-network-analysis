package com.twitter.analysis.config

import org.apache.spark.sql.SparkSession

/** AWS S3 configuration and credential management for Databricks integration
  */
object AWSConfig {

  /** Default AWS region for S3 operations (us-east-1 for optimal performance)
    */
  private val DEFAULT_AWS_REGION = "us-east-1"

  /** S3 configuration settings for optimal large file processing
    */
  private val S3_OPTIMIZATION_SETTINGS = Map(
    "fs.s3a.multipart.size"               -> "134217728", // 128MB multipart size
    "fs.s3a.fast.upload"                  -> "true",
    "fs.s3a.block.size"                   -> "134217728", // 128MB block size
    "fs.s3a.multipart.threshold"          -> "536870912", // 512MB threshold
    "fs.s3a.connection.maximum"           -> "200",
    "fs.s3a.threads.max"                  -> "64",
    "fs.s3a.connection.establish.timeout" -> "5000",
    "fs.s3a.connection.timeout"           -> "200000"
  )

  /** Configures Spark session with AWS S3 settings and IAM role authentication
    * @param spark
    *   The SparkSession to configure
    * @param awsRegion
    *   AWS region for S3 operations (defaults to us-east-1)
    */
  def configureS3Access(spark: SparkSession, awsRegion: String = DEFAULT_AWS_REGION): Unit = {
    val sparkConf = spark.sparkContext.getConf

    // Set AWS region
    sparkConf.set(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    )
    sparkConf.set("spark.hadoop.fs.s3a.endpoint.region", awsRegion)

    // Apply S3 optimization settings
    S3_OPTIMIZATION_SETTINGS.foreach { case (key, value) =>
      sparkConf.set(s"spark.hadoop.$key", value)
    }

    // Enable S3A file system
    sparkConf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkConf.set("spark.hadoop.fs.s3a.path.style.access", "false")
  }
}
