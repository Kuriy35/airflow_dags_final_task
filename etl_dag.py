from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime
import os

with DAG (
    dag_id = 'my_test_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,                     
    catchup=False
) as dag:
    
    raw_data_volume = k8s.V1Volume(name="raw-data-from-sftp",
                        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                            claim_name=os.getenv("PVC_NAME", "raw-data-from-sftp")))

    sftp_executor_config = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                security_context=k8s.V1PodSecurityContext(
                    fs_group=1001,
                ),
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[
                            k8s.V1VolumeMount(
                                name="raw-data-from-sftp",
                                mount_path="/sftp"
                            )
                        ]
                    )
                ],
                volumes=[raw_data_volume]
            )
        )
    }

    get_data_from_sftp = SFTPOperator (
        task_id="get_data_from_sftp",
        ssh_conn_id="sftp_server",
        local_filepath="/sftp/data.*",
        remote_filepath="/sftp/data.*",
        operation="get",
        executor_config=sftp_executor_config
    )
    
    HDFS_HOST=os.getenv("HDFS_HOST", "hdfs-namenodes") + ":" + os.getenv("HDFS_PORT", "8020")
    HADOOP_LOG_DIR=os.getenv("HDFS_LOG_DIR", "/data0/logs")
    
    load_data_to_hdfs = KubernetesPodOperator(
        task_id="load_data_to_hdfs",
        namespace="data",
        image="gchq/hdfs:3.3.3",
        volumes=[raw_data_volume],
        volume_mounts=[k8s.V1VolumeMount(name="raw-data-from-sftp", mount_path="/sftp")],
        cmds=["bash", "-lc"],
        arguments=[f"""
              set -eux
              ls -lh /sftp
              hdfs dfs -fs hdfs://{HDFS_HOST} -mkdir -p /data/raw_data
              hdfs dfs -fs hdfs://{HDFS_HOST} -put -f /sftp/data.* /data/raw_data/
              hdfs dfs -fs hdfs://{HDFS_HOST} -ls -lh /data/raw_data"""],
        env_vars=[k8s.V1EnvVar(name="HADOOP_LOG_DIR", value=HADOOP_LOG_DIR)],
        is_delete_operator_pod=True,
        get_logs=True
    )

    get_data_from_sftp >> load_data_to_hdfs