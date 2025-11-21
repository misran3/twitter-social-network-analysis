# Twitter Social Network Analysis

## Overview

This project analyzes Twitter social networks to find influential users and generate follower recommendations. It runs large-scale graph analytics using Apache Spark to compute PageRank scores and topic-based user recommendations. The system processes Twitter follower relationships and user interests to identify key influencers and suggest connections between users with similar topics like games, movies, and music.

## Tech Architecture

The system runs on AWS using EMR clusters with Spark for distributed processing. All data lives in S3 buckets for storage and results output. The infrastructure gets deployed through AWS CDK, which sets up the EMR cluster, security groups, IAM roles, and S3 buckets automatically.

The Scala applications use Spark's GraphX and SQL libraries to process the social network data. Each analysis type has its own processor that handles the specific algorithms. Results get saved back to S3 in Parquet format for easy querying and further analysis.

## Usage

First, deploy the AWS infrastructure using CDK:

```bash
cd cdk
npm install
cdk deploy
```

This creates your EMR cluster, S3 buckets, and security groups. Once deployed, run the analysis using the provided script:

```bash
chmod +x ./run-emr-step.sh
./run-emr-step.sh
```

The script will build your project, upload the JAR to S3, and let you choose from three analysis types:
1. **Basic Network Analysis** - Graph statistics and top users by follower count
2. **PageRank Analysis** - User influence scoring using PageRank algorithm
3. **User Recommendations** - Follower suggestions based on topic interests

Your input data should be tab-separated files. The edges file contains follower relationships (`source\tdestination`), and the topics file includes user interests (`userId\tgames\tmovies\tmusic`).

Results will be saved in the specified S3 output path in Parquet format.

### Running tests

To run unit tests for the Scala applications, use the following command:

```bash
sbt test
```
