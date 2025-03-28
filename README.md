# EMR Cluster Management with Apache Airflow

This project demonstrates the use of **Apache Airflow** for managing **Amazon EMR clusters**. The workflow includes tasks to create an EMR cluster, add a Spark job, monitor the status of the job, and finally, terminate the cluster.

### Key Features:
- **Create EMR Cluster**: Launch an Amazon EMR cluster with specified configurations.
- **Add Spark Job**: Submit a Spark job to the EMR cluster to run an ETL process.
- **Monitor Spark Job**: Use the `EmrStepSensor` to monitor the status of the Spark job.
- **Terminate EMR Cluster**: Terminate the EMR cluster after the job is complete.

### Dependencies:
- **Apache Airflow**: Workflow orchestrator.
- **Amazon EMR**: Managed cluster service for big data processing.
- **Boto3**: AWS SDK for Python to interact with AWS services.
- **AWS CLI**: Command-line interface to interact with AWS.

### Setup Instructions:

#### 1. Install Dependencies
Create a virtual environment and install the necessary dependencies.

```bash
# Create a virtual environment
python -m venv airflow-env
source airflow-env/bin/activate`

# Install dependencies
pip install -r requirements.txt
```
#### 2. AWS Configuration
Ensure that you have the AWS CLI configured with your AWS credentials.
```bash
aws configure
```
Make sure that your AWS account has the necessary IAM roles and permissions for EMR operations (e.g., `EMRserviceRole` and `AmazonEMR-EC2Role`).
#### 3. Airflow Configuration
Make sure that Airflow is installed and properly configured with the necessary connection to AWS (`aws_default` in this case).

1. **Configure AWS connection in Airflow UI**:
   - Go to the **Airflow UI** (usually at `http://localhost:8085`).
   - Navigate to **Admin** -> **Connections**.
   - Add a new connection with the following parameters:
     - **Conn Id**: `aws_default`
     - **Conn Type**: `Amazon Web Services`
     - **Access Key**: Your AWS Access Key
     - **Secret Key**: Your AWS Secret Key
     - **Region Name**: Your preferred AWS region (e.g., `us-west-2`, `us-east-1`)

#### 4. Running the DAG
Once everything is set up, trigger the DAG manually in the Airflow UI to run the workflow.

1. Go to the **DAGs** section in the Airflow UI.
2. Find **emr_cluster_management** and click the **play** button to start the DAG.

The tasks will execute in sequence:

- **Create EMR Cluster**
- **Check Cluster Status**
- **Add Spark Job**
- **Monitor Job Status**
- **Terminate Cluster**

#### 5. Files and Scripts:
- **DAG File**: `aws_emr_dag.py` — Contains the main DAG definition.
  
- **Helper Function**: `scripts/emr_utils.py` — Contains helper function- `cluster_status_check` used for checking the status of the cluster.

#### Spark Job:
The Spark job being submitted to EMR runs an ETL script stored in an S3 bucket:

* s3://af-emr-****/script/etl_job.py
It processes two input datasets located in S3:

* s3://af-emr-****/input_datas/green_taxi_trip_may_2024.csv
* s3://af-emr-****/input_datas/trip_type.csv

And outputs the results to:
 * s3://af-emr-****/outputs/

### Key Learnings:
 - Using Airflow operators like `EmrCreateJobFlowOperator`, `EmrAddStepsOperator`, and `EmrTerminateJobFlowOperator`.
- Monitoring job execution using `EmrStepSensor`.
- Automating the entire ETL pipeline on EMR with Airflow orchestration.
