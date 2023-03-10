import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

default_args={
   'depends_on_past': False,
   'email': ['abcd@gmail.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}
dag = DAG(
   'dag-spark-hdfs',
   default_args={'max_active_runs': 1},
   description='submit spark-pi as sparkApplication on kubernetes',
   schedule_interval=timedelta(days=1),
   start_date=datetime(2023, 3, 9),
   catchup=False,
)

t1 = SparkKubernetesOperator(
   task_id='hdfs-write',
   retries=1,
   application_file="/opt/airflow/dags/repo/write-hdfs.yaml",
   namespace="default",
   kubernetes_conn_id="myk8s",
   do_xcom_push=True,
   dag=dag,
)
