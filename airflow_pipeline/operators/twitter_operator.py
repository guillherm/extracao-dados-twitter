import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta  
from os.path import join
from pathlib import Path

class TwitterOperator(BaseOperator):

    #construtor
    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path

        super().__init__(**kwargs)

    #criação das pastas de acordo com o file_path = datalake_twitter{query}/extract_date = / query_today.json
    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    #execução do operator
    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query
        
        self.create_parent_folder()

        #escrita no arquivo
        with open(self.file_path, "w") as output_file:
            #paginate
            for pg in TwitterHook(end_time, start_time, query).run():
                json.dump(pg, output_file)
                output_file.write("\n")

if __name__ == "__main__":
    #montando url
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    with DAG(dag_id = "TwitterTest", start_date=datetime.now()):
        to = TwitterOperator(file_path=join(f"datalake/twitter_{query}/"
                                            f"extract_date={datetime.now().date()}",
                                            f"{query}_{datetime.now().date().strftime('%Y%m%d')}.json"),
                            query=query, start_time=start_time, end_time=end_time, task_id="test_run")
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)