package com.twitter.analysis.models

import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

/** Schema definitions for DataFrames
  */
object DataModels {

  /** Schema for edge data containing source and destination user IDs
    */
  val edgeSchema: StructType = StructType(
    Array(
      StructField("source", LongType, nullable = false),
      StructField("destination", LongType, nullable = false)
    )
  )

  /** Schema for topics data containing user posting frequencies
    */
  val topicSchema: StructType = StructType(
    Array(
      StructField("userId", LongType, nullable = false),
      StructField("games", DoubleType, nullable = false),
      StructField("movies", DoubleType, nullable = false),
      StructField("music", DoubleType, nullable = false)
    )
  )
}
