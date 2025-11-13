name := "twitter-social-network-analysis"

version := "1.0.0"

scalaVersion := "2.12.15"

// Spark dependencies
val sparkVersion = "3.5.6"

libraryDependencies ++= Seq(
  // Apache Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  
  // AWS SDK for S3 integration
  "org.apache.hadoop" % "hadoop-aws" % "3.4.1" % "provided",
  "software.amazon.awssdk" % "bundle" % "2.38.2" % "provided",
  
  // Testing dependencies
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
)

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

// Assembly settings for creating fat JARs if needed
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Test settings
Test / parallelExecution := false
Test / fork := true