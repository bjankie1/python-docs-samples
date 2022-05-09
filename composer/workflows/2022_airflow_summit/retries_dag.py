# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START composer_metadb_cleanup]
"""
A DAG that demonstrates retries capabilities of Apache Airflow. It is meant
to be used for training purposes during Airflow Summit 2022 workshop.
## Usage
1. Update the global variables (NUMBER_OF_RETRIES, RETRY_DELAY_SECONDS) in 
the DAG with the desired values
2. Put the DAG in your gcs bucket.
"""
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from random import randint

from tenacity import retry

# How many times each task should be retried before it fails permanently.
NUMBER_OF_RETRIES = 3

# Number of seconds to wait before retry.
RETRY_DELAY_SECONDS = 1

args = {
    'start_date': days_ago(1),
    'retries': NUMBER_OF_RETRIES,
    'retry_delay': timedelta(seconds=RETRY_DELAY_SECONDS)
}

dag = DAG(dag_id='retries_example', default_args=args)


def apiCall(parameter):
    if randint(1, 2) == 2:
        raise RuntimeError("Error to simulate API flakiness")
    print("Calling API with parameter " + str(parameter))


with dag:
    task_1 = PythonOperator(
        task_id='task1',
        python_callable=apiCall,
        op_kwargs={'parameter': 1}
    )
    task_2 = PythonOperator(
        task_id='task2',
        python_callable=apiCall,
        op_kwargs={'parameter': 2}
    )
    task_3 = PythonOperator(
        task_id='task3',
        python_callable=apiCall,
        op_kwargs={'parameter': 3}
    )
    task_4 = PythonOperator(
        task_id='task4',
        python_callable=apiCall,
        op_kwargs={'parameter': 4}
    )

# t2,t3 runs parallel right after t1 has ran
task_1 >> [task_2, task_3] >> task_4
