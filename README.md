# Pinterest Data Pipeline Setup
## Table of contents
## Overview
This project involves setting up a data pipeline for Pinterest data using Amazon Managed Streaming for Apache Kafka (MSK) and Amazon EC2 on the AWS cloud. The pipeline will handle Pinterest posts data, post geolocation data, and post user data.

## Milestone 1: GitHub and AWS Account Setup

### Task 1: GitHub Repo Creation
1. Click the button on the right to create a new GitHub repo.
2. Use the created GitHub repo to track changes to your code.

### Task 2: AWS Account Creation
1. Click the button on the right to create a new AWS cloud account.
2. Follow the instructions received via email to log in to the AWS Console.
3. Note down your login credentials and ensure you're in the us-east-1 region.

## Milestone 2: Infrastructure Setup and Data Exploration

### Task 1: Infrastructure Setup
1. Download the zip package from the provided link.
2. Run the `user_posting_emulation.py` script to explore the provided data.
3. Familiarize yourself with the `pin_result`, `geo_result`, and `user_result` data entries.

### Task 2: AWS Console Login
1. Navigate to the AWS Console and log in using the provided credentials.
2. Choose a new password and make a note of it along with your UserID.
3. Ensure you're in the `us-east-1` region for all services.

## Milestone 3: AWS EC2 and MSK Cluster Configuration

### Task 1: EC2 Key Pair Configuration
1. Create a `.pem` file containing your EC2 key pair locally.
2. Associate the key pair with your EC2 instance on the AWS EC2 console.

### Task 2: EC2 Instance Connection
1. Follow the SSH client connection instructions to connect to your EC2 instance.

### Task 3: MSK Cluster Authentication
1. Install Kafka and IAM MSK authentication packages on your EC2 client.
2. Obtain the IAM role ARN from the IAM console for EC2 access.
3. Modify the `client.properties` file for Kafka client authentication.

### Task 4: Kafka Topic Creation
1. Retrieve the MSK cluster Bootstrap servers and Zookeeper connection strings from the MSK Management Console.
2. Create the following Kafka topics using the obtained information:
   - `<your_UserId>.pin`
   - `<your_UserId>.geo`
   - `<your_UserId>.user`

For this project, I created three topics. One each for the pinterest_data, geolocation_data, and user_data outlined above.
./kafka-topics.sh --create --topic 1273a52843e9pin --bootstrap-server b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --partitions 5 --replication-factor 3
./kafka-topics.sh --create --topic 1273a52843e9geo --bootstrap-server b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --partitions 5 --replication-factor 3
./kafka-topics.sh --create --topic 1273a52843e9user --bootstrap-server b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --partitions 5 --replication-factor 3

