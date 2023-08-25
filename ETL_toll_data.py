# Import libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


# Task 1.1 Defining the DAG arguments

default_args = {
    'owner': 'Wb Az',
    'start_date': days_ago(0),
    'email':['wbaz@acemail.com'],
    'email_on_failure': True,
    'email_on_try': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}

# Task 1.2 Define the Dag

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),)

##### Creating Tasks #####

# Task 1.3 Unzip data

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C \
            /home/project/airflow/dags/finalassignment',
    dag=dag,)

# Task 1.4 Extract from csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv >\
                /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# Task 1.5 Extract from tsv

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv \
           |tr "\t" "," > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

# Task 1.6 Extract data from fixed width

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -b59-62,63-67 /home/project/airflow/dags/finalassignment/payment-data.txt | \
                tr ' ' ',' > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag,
)

# Task 1.7 Consolidate data

consolidate_data =  BashOperator(
    task_id='consolidate_data',
    bash_command = 'paste -d "," /home/project/airflow/dags/finalassignment/csv_data.csv \
                /home/project/airflow/dags/finalassignment/tsv_data.csv \
                /home/project/airflow/dags/finalassignment/fixed_width_data.csv > \
                 tr -d "\r" > /home/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# Task 1.8 Transform and load data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='cut -d "," -f4 /home/airflow/dags/finalassignment/extracted_data.csv \
                |tr "[a-z]" "[A-Z]" > /home/airflow/dags/finalassignment/transformed_data.csv',
    dag=dag,
)

### Define the task pipeline ###

# Task 1.9
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data