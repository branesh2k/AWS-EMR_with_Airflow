import time
import boto3

client = boto3.client('emr')

def cluster_status_check(ti):
    cluster_id = ti.xcom_pull(task_ids = 'create_emr_cluster')
    response = client.describe_cluster(ClusterId = cluster_id)
    cluster_status = response['Cluster']['Status']['State']

    while cluster_status not in ['TERMINATED']:
        print(f"Cluster status is {cluster_status}.. Retrying in 30 seconds..")
        time.sleep(30)
        response = client.describe_cluster(ClusterId=cluster_id)
        cluster_status = response['Cluster']['Status']['State']

        if cluster_status == 'WAITING':
            break

    if cluster_status == 'TERMINATED':
        raise ValueError("Cluster terminated unexpectedly.")
        
    print(f"Cluster status is {cluster_status} and ready for Spark-Submit/Step.")

