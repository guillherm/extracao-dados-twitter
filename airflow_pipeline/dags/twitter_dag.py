import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from airflow.utils.dates import days_ago
    
with DAG(dag_id = "TwitterDAG", start_date=days_ago(60), schedule_interval="@daily") as dag:
    
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"
    to = TwitterOperator(file_path=join(f"datalake/twitter_{query}",
                                            "extract_date={{ ds }}",
                                            "datascience_{{ ds_nodash }}.json"),
                            query=query, 
                            start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
                            end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
                            task_id=f"twitter_{query}")
 
    """twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience",
                                            application="/home/guilherme/extracao-dados-twitter/src/spark/transformation.py",
                                            name="twitter_transformation",
                                            application_args=["--src","/home/guilherme/extracao-dados-twitter/datalake/twitter_datascience",
                                                              "--dest","/home/guilherme/extracao-dados-twitter/dados_transformation",
                                                              "--process-date","{{ ds }}"])"""
    
#twitter_operator >> twitter_transform