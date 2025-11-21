#!/bin/bash

# Social Network Analysis EMR Step Runner
# This script builds the project, retrieves stack outputs, and creates EMR steps

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[info]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[success]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[warn]${NC} $1"
}

log_error() {
    echo -e "${RED}[error]${NC} $1"
}

# Environment checks
log_info "Performing environment checks..."

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed or not in PATH"
    exit 1
fi
log_success "AWS CLI found: $(aws --version)"

# Verify AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "AWS CLI is not configured properly. Please set up your credentials."
    exit 1
fi
log_success "AWS credentials are configured"

# Check SBT
if ! command -v sbt &> /dev/null; then
    log_error "SBT is not installed or not in PATH"
    exit 1
fi
log_success "SBT found: $(sbt --version | head -n1)"

# Check if we're in the project directory
if [ ! -f "build.sbt" ]; then
    log_error "build.sbt not found. Please run this script from the project root directory."
    exit 1
fi

# Get stack outputs
log_info "Retrieving CDK stack outputs..."

STACK_NAME="SocialNetworkAnalysis"
get_stack_output() {
    aws cloudformation describe-stacks \
      --stack-name "$STACK_NAME" \
      --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
      --output text \
      2>/dev/null || echo ""
}

#BUCKET_NAME=$(get_stack_output "BucketName")
#CLUSTER_ID=$(get_stack_output "ClusterId")
BUCKET_NAME="socialnetworkanalysis-socialnetworkanalysisbucketa-gfgidwepbvd4"
CLUSTER_ID="j-H49B3HJEJ2Q6"

if [ -z "$BUCKET_NAME" ]; then
    log_error "Could not retrieve output: BucketName from CloudFormation Stack: $STACK_NAME"
    exit 1
fi

if [ -z "$CLUSTER_ID" ]; then
    log_error "Could not retrieve output: BucketName from CloudFormation Stack: $STACK_NAME"
    exit 1
fi

log_success "Retrieved stack outputs:"
log_info "  Bucket Name: $BUCKET_NAME"
log_info "  Cluster ID: $CLUSTER_ID"

# Check EMR cluster status
log_info "Checking EMR cluster status..."
CLUSTER_STATUS=$(aws emr describe-cluster --cluster-id "$CLUSTER_ID" --query 'Cluster.Status.State' --output text)
if [[ "$CLUSTER_STATUS" != "WAITING" && "$CLUSTER_STATUS" != "RUNNING" ]]; then
    log_error "EMR Cluster $CLUSTER_ID is not in a valid state (current state: $CLUSTER_STATUS). It must be in WAITING or RUNNING state."
#    exit 1
fi
log_success "EMR Cluster $CLUSTER_ID is in state: $CLUSTER_STATUS"

log_success "Environment checks passed!"

# Build the project
log_info "Building project with sbt clean assembly..."
sbt clean assembly

if [ $? -ne 0 ]; then
    log_error "Build failed!"
    exit 1
fi
log_success "Project built successfully"

# Driver class selection
echo
log_info "Available driver classes:"
echo "  1) SocialNetworkAnalysis - Basic graph analysis with top users"
echo "  2) PageRankAnalysis - PageRank algorithm for user influence"
echo "  3) UserRecommendationAnalysis - Follower recommendations based on topics"

while true; do
    read -p "Select driver class (1-3): " choice
    case $choice in
        1)
            DRIVER_CLASS="com.twitter.analysis.SocialNetworkAnalysis"
            JOB_NAME="Twitter Social Network Analysis Job"
            break
            ;;
        2)
            DRIVER_CLASS="com.twitter.analysis.PageRankAnalysis"
            JOB_NAME="Twitter PageRank Analysis Job"
            break
            ;;
        3)
            DRIVER_CLASS="com.twitter.analysis.UserRecommendationAnalysis"
            JOB_NAME="Twitter Recommendation Analysis Job"
            break
            ;;
        *)
            log_warning "Invalid selection. Please choose 1, 2, or 3."
            ;;
    esac
done

log_success "Selected: $DRIVER_CLASS"

# Get additional parameters based on driver class
echo
case $choice in
    1)
        log_info "SocialNetworkAnalysis parameters:"
        read -p "Enter input path (default: s3a://$BUCKET_NAME/data/Graph): " input_path
        input_path=${input_path:-"s3a://$BUCKET_NAME/data/Graph"}

        read -p "Enter output path (default: s3a://$BUCKET_NAME/output): " output_path
        output_path=${output_path:-"s3a://$BUCKET_NAME/output"}

        read -p "Enter topN users (default: 100): " top_n
        top_n=${top_n:-100}

        ARGS=("$input_path" "$output_path" "$top_n")
        ;;
    2)
        log_info "PageRankAnalysis parameters:"
        read -p "Enter input path (default: s3a://$BUCKET_NAME/data/Graph): " input_path
        input_path=${input_path:-"s3a://$BUCKET_NAME/data/Graph"}

        read -p "Enter output path (default: s3a://$BUCKET_NAME/output): " output_path
        output_path=${output_path:-"s3a://$BUCKET_NAME/output"}

        read -p "Enter iterations (default: 10): " iterations
        iterations=${iterations:-10}

        read -p "Enter topN users (default: 100): " top_n
        top_n=${top_n:-100}

        ARGS=("$input_path" "$output_path" "$iterations" "$top_n")
        ;;
    3)
        log_info "UserRecommendationAnalysis parameters:"
        read -p "Enter graph path (default: s3a://$BUCKET_NAME/data/Graph): " graph_path
        graph_path=${graph_path:-"s3a://$BUCKET_NAME/data/Graph"}

        read -p "Enter topics path (default: s3a://$BUCKET_NAME/data/Graph-Topics): " topics_path
        topics_path=${topics_path:-"s3a://$BUCKET_NAME/data/Graph-Topics"}

        read -p "Enter output path (default: s3a://$BUCKET_NAME/output): " output_path
        output_path=${output_path:-"s3a://$BUCKET_NAME/output"}

        read -p "Enter iterations (default: 10): " iterations
        iterations=${iterations:-10}

        ARGS=("$graph_path" "$topics_path" "$output_path" "$iterations")
        ;;
esac

# Generate step JSON
JAR_PATH="s3://$BUCKET_NAME/jars/twitter-social-network-analysis-assembly-1.0.0.jar"

# Copy JAR to S3 (if not already present)
log_info "Uploading JAR to S3..."
aws s3 cp target/scala-2.12/twitter-social-network-analysis-assembly-1.0.0.jar "$JAR_PATH"
log_success "JAR uploaded to $JAR_PATH"

# Build the step JSON
STEP_JSON=$(cat <<EOF
[
  {
    "Type": "Spark",
    "Name": "$JOB_NAME",
    "ActionOnFailure": "CONTINUE",
    "Args": [
      "--class",
      "$DRIVER_CLASS",
      "--deploy-mode",
      "client",
      "--driver-memory",
      "8g",
      "--driver-cores",
      "3",
      "--executor-memory",
      "8g",
      "--executor-cores",
      "4",
      "--num-executors",
      "5",
      "--conf",
      "spark.executor.memoryOverhead=1600m",
      "--conf",
      "spark.driver.memoryOverhead=1600m",
      "--conf",
      "spark.default.parallelism=80",
      "--conf",
      "spark.sql.shuffle.partitions=80",
      "--conf",
      "spark.sql.files.maxPartitionBytes=134217728",
      "--conf",
      "spark.sql.autoBroadcastJoinThreshold=10485760",
      "--conf",
      "spark.network.timeout=800s",
      "--conf",
      "spark.executor.heartbeatInterval=60s",
      "--conf",
      "spark.dynamicAllocation.enabled=false",
      "--conf",
      "spark.speculation=false",
      "--conf",
      "spark.serializer=org.apache.spark.serializer.KryoSerializer",
      "--conf",
      "spark.kryoserializer.buffer.max=512m",
      "--conf",
      "spark.hadoop.fs.s3a.connection.maximum=200",
      "--conf",
      "spark.hadoop.fs.s3a.threads.max=100",
      "--conf",
      "spark.hadoop.fs.s3a.fast.upload=true",
      "--conf",
      "spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer",
      "--conf",
      "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
      "$JAR_PATH"$(for arg in "${ARGS[@]}"; do echo ","; echo "      \"$arg\""; done)
    ]
  }
]
EOF
)

# Show the generated step JSON for confirmation
echo
log_info "Generated EMR step JSON:"
echo "$STEP_JSON"

echo
read -p "Do you want to create this EMR step? (y/N): " confirm
if [[ ! $confirm =~ ^[Yy]$ ]]; then
    log_warning "Step creation cancelled."
    exit 0
fi

# Create the EMR step
log_info "Creating EMR step..."

# Write step JSON to temporary file
TEMP_STEP_FILE=$(mktemp)
echo "$STEP_JSON" > "$TEMP_STEP_FILE"

# Add the step to the EMR cluster
STEP_ID=$(aws emr add-steps --cluster-id "$CLUSTER_ID" --steps file://"$TEMP_STEP_FILE" --query 'StepIds[0]' --output text)

# Clean up temp file
rm "$TEMP_STEP_FILE"

if [ $? -eq 0 ]; then
    log_success "EMR step created successfully!"
    log_info "Step ID: $STEP_ID"
    log_info "Cluster ID: $CLUSTER_ID"
    echo
    log_info "You can monitor the step status using:"
    echo "  aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID"
    echo
    log_info "Or check the EMR console:"
    echo "  https://console.aws.amazon.com/emr/home#/clusters"
else
    log_error "Failed to create EMR step"
    exit 1
fi