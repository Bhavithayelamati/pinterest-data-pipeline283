# Pinterest Data Pipeline Setup

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

## Milestone 4: Batch Processing: Connect a MSK cluster to a S3 bucket
You will use MSK Connect to connect the MSK cluster to a S3 bucket, such that any data going through the cluster will be automatically saved and stored in a dedicated S3 bucket.
### Task 1: Create a custom plugin with MSK Connect
1. Navigate to the MSK Connect service in the AWS Management Console.
2. Choose "Create a custom plugin" and follow the on-screen instructions to set it up. Ensure the plugin is compatible with your Kafka version and configured to route data to S3.

### Task 2: Create a Connector with MSK Connect
1. With your custom plugin ready, create a new connector in MSK Connect.
2. Configure the connector by specifying your MSK cluster and S3 bucket details. Ensure the connector uses the custom plugin created in the previous step.
3. Launch the connector and monitor its status to ensure a successful connection between your MSK cluster and S3 bucket.

## Milestone 5: Batch Processing - Configuring an API in API Gateway
To replicate Pinterest's experimental data pipeline, we'll build our API. This API will forward data to the MSK cluster, which then gets stored in the S3 bucket via the previously established connector.
### Task 1: Build a Kafka REST Proxy Integration Method for the API
1. In the AWS Management Console, navigate to API Gateway.
2. Create a new API and define a resource and method for Kafka REST proxy integration. This setup will allow API calls to send data directly to your Kafka topics.

### Task 2: Set Up the Kafka REST Proxy on the EC2 Client
1. Install and configure the Kafka REST Proxy on your EC2 instance, ensuring it's connected to your MSK cluster.
2. Test the proxy to ensure it can communicate with Kafka topics successfully.

### Task 3: Send Data to the API
#### Step 1: Modify the `user_posting_emulation.py` Script
- Adjust the script to send data to your Kafka topics using your API's Invoke URL. Ensure data from the `pin_result`, `geo_result`, and `user_result` tables are sent to their respective Kafka topics.
#### Step 2: Check Data Transmission to the Cluster
- Run a Kafka consumer for each topic to verify that messages are being received. If set up correctly, you should see the messages as they are consumed.
#### Step 3: Verify Data Storage in S3 Bucket
- Check your S3 bucket for the presence of new data. Pay attention to the folder organization, which should reflect the structure of the data being ingested from Kafka.

## Milestone 6: Batch Processing - Databricks
In this milestone, you will set up your Databricks account and prepare for data processing by mounting an S3 bucket to Databricks.
### Task 1: Set Up Your Own Databricks Account
1. Sign up or log into your Databricks account.
2. Familiarize yourself with the Databricks interface and available documentation.
### Task 2: Mount an S3 Bucket to Databricks
1. Access the Databricks console and navigate to your workspace.
2. Use the Databricks CLI or the notebook interface to mount the S3 bucket containing your data.
3. Ensure you can access the data stored in S3 from your Databricks workspace.

## Milestone 7: Batch Processing - Spark on Databricks
Learn how to perform data cleaning and computations using Spark on Databricks.
### Task 1 to Task 11: Data Cleaning and Analysis
1. **Clean DataFrames**: Clean the dataframes containing Pinterest posts, geolocation, and user data.
2. **Analysis**:
   - Find the most popular category in each country.
   - Determine the most popular category each year.
   - Identify the user with the most followers in each country.
   - Analyze the most popular category for different age groups.
   - Calculate the median follower count for different age groups.
   - Count how many users have joined each year.
   - Find the median follower count of users based on their joining year and age group.

## Milestone 8: Batch Processing - AWS MWAA
Automate the execution of your Databricks notebooks using Apache Airflow managed by AWS MWAA.
### Task 1: Create and Upload a DAG to a MWAA Environment
1. Develop an Airflow DAG that triggers your Databricks notebook to run on a daily schedule.
2. Upload the DAG file to the `dags` folder in the `mwaa-dags-bucket` using the AWS S3 console or CLI. Name your DAG file `<your_UserId_dag.py>` to adhere to the permission model.
### Task 2: Trigger a DAG That Runs a Databricks Notebook
1. Navigate to the MWAA console and locate your environment.
2. Manually trigger the DAG you've uploaded and monitor its execution to ensure it runs successfully.

## Milestone 9: Stream Processing with AWS Kinesis
This milestone involves setting up AWS Kinesis streams for real-time data processing of Pinterest data, integrating these streams with an API, and using Databricks for stream analysis.
### Task 1: Create Data Streams Using Kinesis Data Streams
1. **Create Three Data Streams**:
   - Use AWS Kinesis Data Streams to create three separate streams for Pinterest data tables: pins, geolocation, and user data.
   - Name the streams as follows:
     - `streaming-<your_UserId>-pin`
     - `streaming-<your_UserId>-geo`
     - `streaming-<your_UserId>-user`
   - Ensure the stream names follow the specified nomenclature to avoid permission errors.
### Task 2: Configure an API with Kinesis Proxy Integration
1. **Modify Your REST API**:
   - Integrate your API with Kinesis to invoke actions such as listing streams, creating, describing, deleting streams, and adding records.
   - Use the access role `ARN` provided to you (`<your_UserId-kinesis-access-role>`) for the execution role in the integration setup.
### Task 3: Send Data to the Kinesis Streams
1. **Develop a Streaming Script**:
   - Create `user_posting_emulation_streaming.py`, enhancing the initial `user_posting_emulation.py`.
   - The script should make API calls to add records individually to the corresponding streams for each Pinterest data table.
### Task 4: Read Data from Kinesis Streams in Databricks
1. **Databricks Notebook Setup**:
   - Create a new notebook in Databricks.
   - Read in AWS credentials from `dbfs:/user/hive/warehouse/authentication_credentials` to access the Kinesis streams.
2. **Data Ingestion**:
   - Verify that your data streams are correctly receiving data by checking the Kinesis console.
3. **Stream Reading**:
   - Implement stream reading in your Databricks notebook for the three created streams.
### Task 5: Transform Kinesis Streams in Databricks
1. **Clean Streaming Data**:
   - Apply the same data cleaning processes used for batch data to the streaming data in Databricks.
### Task 6: Write the Streaming Data to Delta Tables
1. **Save Cleaned Data**:
   - After cleaning, save the streaming data to Delta Tables within Databricks.
   - Name the tables as follows:
     - `<your_UserId>_pin_table`
     - `<your_UserId>_geo_table`
     - `<your_UserId>_user_table`
