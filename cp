# import sys
# import os


# sys.path.insert(0,os.path.dirname(os.path.abspath("/opt/airflow/dags/utils/bootstrap.py")))
# sys.path.insert(0,os.path.dirname(os.path.abspath("/opt/airflow/dags/utils/wrapper_util.py")))
# sys.path.insert(0,os.path.dirname(os.path.abspath("/opt/airflow/dags/constants/process.py")))
# sys.path.insert(0,os.path.dirname(os.path.abspath("/opt/airflow/dags/utils/process_run_util.py")))

from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# from airflow.providers.cncf.kubernetes.operators.kubernetes_delete_custom_object import KubernetesDeleteCustomObjectOperator
from airflow.providers.cncf.kubernetes.operators.resource import KubernetesDeleteResourceOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import os

# Get the directory where the current DAG file is located
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
 
from airflow import DAG
from datetime import datetime

from utils.bootstrap import bootstrap_process_group
from airflow.operators.python import PythonOperator
from utils.process_run_util import end_process_success,fail_process_run
from utils.wrapper_util import stage_wrapper
from constants.process import Process
from constants.assets import GLIF_ALL_MANIFEST_ASSET,CBS_ALL_MANIFEST_ASSET,CONTROL_MANIFEST_ASSET

with DAG(

    dag_id="control_file_dag",
    start_date=datetime(2024, 1, 1),# schedule=None,
    catchup=False,

) as dag:
    bootstrap = bootstrap_process_group(dag=dag,process_id=Process.CONTROL_PIPELINE) ##process_id=3 ie difference pipeline
    



    run_control_file_spark = stage_wrapper(
            dag=dag,
            stage_id="control_file_spark",
            operator_cls=KubernetesPodOperator,
            operator_kwargs={
              "pod_template_file": os.path.join(DAG_DIR, "control_file.yaml"),
                "namespace": "{{ var.value.get('fincore_namespace', 'cbops') }}",
                "image": "{{ var.value.get('fincore_spark_image_name', 'h06vksharbor.corp.ad.sbi/cbops/prodetl:v1') }}",
                "kubernetes_conn_id": "k8s_conns",
                "on_finish_action": "delete_pod",  # Deletes on both Success & Failure

            },
            stage_meta={"type": "SPARK", "label": "Control Spark Job"},
        )
    
    mark_process_failure = PythonOperator(
        task_id="end_process_failure",
        python_callable=fail_process_run,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        )
    
    mark_process_success = PythonOperator(
        task_id="end_process_success",
        python_callable=end_process_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        outlets = [CONTROL_MANIFEST_ASSET]
    )
    
    
    bootstrap >> run_control_file_spark 
    run_control_file_spark.stage_success 


    [run_control_file_spark.stage_success] >> mark_process_success
    [run_control_file_spark.stage_failure] >> mark_process_failure
    
