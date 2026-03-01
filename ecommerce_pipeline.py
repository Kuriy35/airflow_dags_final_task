from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

NAMESPACE = Variable.get("NAMESPACE", "data")
SFTP_HOST = Variable.get("SFTP_HOST", "sftp")
HDFS_WEB_URL = Variable.get("HDFS_WEB_URL", "http://hdfs-namenodes:9870")
HDFS_NAMENODE = Variable.get("HDFS_NAMENODE", "hdfs-namenodes:8020")
SPARK_MASTER = Variable.get("SPARK_MASTER", "spark://spark-master-svc:7077")

with DAG (
    dag_id = "ecommerce_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    generate_batch_data = KubernetesPodOperator(
        task_id="generate_batch_data",
        namespace=NAMESPACE,
        image="kuriy/ecommerce-batch-generator:latest",
        env_vars=[
            k8s.V1EnvVar(name="SFTP_HOST", value=SFTP_HOST),
            k8s.V1EnvVar(name="SFTP_USERNAME", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="sftp-credentials",
                    key="username"
                )
            )),
            k8s.V1EnvVar(name="SFTP_PASSWORD", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="sftp-credentials",
                    key="password"
                )
            ))
        ],
        is_delete_operator_pod=False,
        get_logs=True
    )

    sftp_to_hdfs = KubernetesPodOperator(
        task_id="sftp_to_hdfs",
        namespace=NAMESPACE,
        image="kuriy/ecommerce-sftp-to-hdfs:latest",
        env_vars=[
            k8s.V1EnvVar(name="SFTP_HOST", value=SFTP_HOST),
            k8s.V1EnvVar(name="SFTP_USERNAME", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="sftp-credentials",
                    key="username"
                )
            )),
            k8s.V1EnvVar(name="SFTP_PASSWORD", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="sftp-credentials",
                    key="password"
                )
            )),
            k8s.V1EnvVar(name="HDFS_WEB_URL", value=HDFS_WEB_URL)
        ],
        is_delete_operator_pod=False,
        get_logs=True
    )

    convert_to_parquet = KubernetesPodOperator(
        task_id="convert_to_parquet",
        namespace=NAMESPACE,
        image="kuriy/ecommerce-spark-transform:latest",
        cmds=["bash", "-lc"],
        arguments=[f"""
            set -e
            
            /opt/bitnami/spark/bin/spark-submit \
            --master {SPARK_MASTER} \
            --deploy-mode client \
            --conf spark.driver.host=$POD_IP \
            --conf spark.executor.memory=512M \
            --conf spark.executor.cores=1 \
            /opt/bitnami/spark/jobs/convert_to_parquet.py
        """],
        env_vars=[
            k8s.V1EnvVar(name="HDFS_NAMENODE", value=HDFS_NAMENODE),
            k8s.V1EnvVar(name="SRC_PATH", value="/data/ecommerce/raw_data"),
            k8s.V1EnvVar(name="DST_PATH", value="/data/ecommerce/converted_data")
        ],
        is_delete_operator_pod=False,
        get_logs=True
    )

    transform_and_load_to_db = KubernetesPodOperator(
        task_id="transform_and_load_to_db",
        namespace=NAMESPACE,
        image="kuriy/ecommerce-spark-transform:latest",
        cmds=["bash", "-lc"],
        arguments=[f"""
            set -e
            
            /opt/bitnami/spark/bin/spark-submit \
            --master {SPARK_MASTER} \
            --conf spark.driver.host=$POD_IP \
            --packages org.postgresql:postgresql:42.7.3 \
            --conf spark.executor.memory=512M \
            --conf spark.executor.cores=1 \
            /opt/bitnami/spark/jobs/transform.py
        """],
        env_vars=[
            k8s.V1EnvVar(name="HDFS_NAMENODE", value=HDFS_NAMENODE),
            k8s.V1EnvVar(name="SRC_PATH", value="/data/ecommerce/converted_data"),
            k8s.V1EnvVar(name="DB_HOST", value="postgresql"),
            k8s.V1EnvVar(name="DB_PORT", value="5432"),
            k8s.V1EnvVar(name="DB_USERNAME", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="postgresql-credentials",
                    key="username"
                )
            )),
            k8s.V1EnvVar(name="DB_PASSWORD", value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="postgresql-credentials",
                    key="password"
                )
            )),
        ],
        is_delete_operator_pod=False,
        get_logs=True
    )

    generate_batch_data >> sftp_to_hdfs >> convert_to_parquet >> transform_and_load_to_db