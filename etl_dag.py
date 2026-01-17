from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime
import fnmatch

with DAG (
    dag_id = 'my_test_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,                     
    catchup=False
) as dag:
    # PVC_NAME = Variable.get("PVC_NAME", default_var="raw-data-from-sftp")
    # raw_data_volume = k8s.V1Volume(name="raw-data-from-sftp",
    #                     persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
    #                         claim_name=PVC_NAME))

    # sftp_executor_config = {
    #     "pod_override": k8s.V1Pod(
    #         spec=k8s.V1PodSpec(
    #             security_context=k8s.V1PodSecurityContext(
    #                 fs_group=1001,
    #             ),
    #             containers=[
    #                 k8s.V1Container(
    #                     name="base",
    #                     volume_mounts=[
    #                         k8s.V1VolumeMount(
    #                             name="raw-data-from-sftp",
    #                             mount_path="/sftp"
    #                         )
    #                     ]
    #                 )
    #             ],
    #             volumes=[raw_data_volume]
    #         )
    #     )
    # }

    # def download_data_files(remote_dir, local_dir, ssh_conn_id):
    #     hook = SFTPHook(ssh_conn_id=ssh_conn_id)
    #     all_files = hook.list_directory(remote_dir)
        
    #     matched_files = [f for f in all_files if f.startswith("data.")]
    #     if not matched_files:
    #         print(f"Жодного файлу 'data.*' не знайдено в {remote_dir}")
    #         return

    #     for file_name in matched_files:
    #         remote_path = f"{remote_dir}/{file_name}"
    #         local_path = f"{local_dir}/{file_name}"
    #         print(f"Loading: {remote_path} -> {local_path}")
    #         hook.retrieve_file(remote_path, local_path)

    # get_data_from_sftp = PythonOperator (
    #     task_id="get_data_from_sftp",
    #     python_callable=download_data_files,
    #     op_kwargs={
    #         "remote_dir": "/sftp",
    #         "local_dir": "/sftp",
    #         "ssh_conn_id": "sftp_server"
    #     },
    #     executor_config=sftp_executor_config
    # )
    
    # HDFS_FULL_URL=Variable.get("HDFS_HOST", default_var="hdfs-namenodes") + \
    #     ":" + Variable.get("HDFS_PORT", default_var="8020")
    # HADOOP_LOG_DIR=Variable.get("HDFS_LOG_DIR", default_var="/data0/logs")
    
    # load_data_to_hdfs = KubernetesPodOperator(
    #     task_id="load_data_to_hdfs",
    #     namespace="data",
    #     image="gchq/hdfs:3.3.3",
    #     volumes=[raw_data_volume],
    #     volume_mounts=[k8s.V1VolumeMount(name="raw-data-from-sftp", mount_path="/sftp")],
    #     cmds=["bash", "-lc"],
    #     arguments=[f"""
    #           set -eux
    #           ls -lh /sftp
    #           hdfs dfs -fs hdfs://{HDFS_FULL_URL} -mkdir -p /data/raw_data
    #           hdfs dfs -fs hdfs://{HDFS_FULL_URL} -put -f /sftp/data.* /data/raw_data/"""],
    #     env_vars=[k8s.V1EnvVar(name="HADOOP_LOG_DIR", value=HADOOP_LOG_DIR)],
    #     is_delete_operator_pod=True,
    #     get_logs=True
    # )

    SPARK_MASTER_FULL_URL = Variable.get("SPARK_MASTER_HOST", default_var="spark-master-svc") + \
        ":" + Variable.get("SPARK_MASTER_PORT", default_var="7077")

    transform_csv_to_parquet = KubernetesPodOperator(
        task_id="transform_csv_to_parquet",
        namespace="data",
        image="kuriy/transform-hdfs-file:latest",
        cmds=["bash", "-lc"],
        arguments=[f"""
            /opt/bitnami/spark/bin/spark-submit \
            --master {SPARK_MASTER_FULL_URL} \
            --deploy-mode client \
            --conf spark.driver.host=$POD_IP \
            /opt/bitnami/spark/jobs/spark-hdfs-job.py"""],
        env_vars=[
            k8s.V1EnvVar(
                name="POD_IP",
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")
                )
            ),
            k8s.V1EnvVar(name="HDFS_HOST", value="hdfs-namenodes"),
            k8s.V1EnvVar(name="HDFS_PORT", value="8020")
        ],
        is_delete_operator_pod=True,
        get_logs=True
    )

    # load_to_postgresql = KubernetesPodOperator

    # get_data_from_sftp >> load_data_to_hdfs >> 
    transform_csv_to_parquet