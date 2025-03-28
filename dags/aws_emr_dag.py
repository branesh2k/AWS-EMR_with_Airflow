from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from scripts.emr_utils import cluster_status_check

# cluster details
JOB_FLOW_OVERRIDES = {
    "Name": "EMR with airflow",
    "ReleaseLabel": "emr-7.8.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Instances": {
        "Ec2SubnetId": "subnet-06921e5fee25026bc",
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "c5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "c5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "AmazonEMR-InstanceProfile-20250311T191608",
    "ServiceRole": "EMRserviceRole",
}

spark_step = {
    "Name": "Spark Job",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit", "--deploy-mode", "cluster", "s3://af-emr-branesh/script/etl_job.py", 
         "--source_input1", "s3://af-emr-branesh/input_datas/green_taxi_trip_may_2024.csv",
         "--source_input2", "s3://af-emr-branesh/input_datas/trip_type.csv",
         "--output_location", "s3://af-emr-branesh/outputs/"],
    },
}
with DAG(
    dag_id="emr_cluster_management",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
    )

    check_cluster_status_task = PythonOperator(
        task_id="check_cluster_status",
        python_callable=cluster_status_check,
    )

    add_spark_job_task = EmrAddStepsOperator(
        task_id = "add_spark_job",
        job_flow_id = "{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id = "aws_default",
        steps = [spark_step],
    )
    step_status_task = EmrStepSensor(
        task_id = 'step_status',
        job_flow_id = "{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id = "{{ ti.xcom_pull(task_ids='add_spark_job')[0] }}",
        aws_conn_id = 'aws_default',
        poke_interval = 30,
        timeout = 300,
        retries = 1,
        retry_delay = timedelta(seconds=10),
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_emr_cluster >> check_cluster_status_task >> add_spark_job_task >> step_status_task >> terminate_emr_cluster