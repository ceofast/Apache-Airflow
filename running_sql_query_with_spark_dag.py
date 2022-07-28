from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator 

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%y-%m-%d')

print(yesterday_date)

default_args = {
		'owner':'train',
		'start_date': yesterday_date,
		'retries':1,
		'retry_delay': timedelta(seconds=5)
}

with DAG('simple_spark_dag', default_args = default_args, schedule_interval = '@daily', catchup = False, template_searchpath = ['home/train/pythonProject/dags/']) as dag:

		t1 = BashOperator(task_id = 'download_data', bash_command = 'wget https://github.com/erkansirin78/datasets/raw/master/dirty_store_transactions.csv -O /tmp/sql_filesdirty_store_transactions.csv')

		t2 = FileSensor(task_id = 'check_file_exists', filepath = '/tmp/dirty_store_transactions.csv')
		# ssh_conn_id için airflow arayüzünü açıyoruz, Admin'i tıklayıp connections'a geliyoruz, + işaretini tıklayarak kendimize ssh_connector yaratıyoruz.
		# Conn Id: my_ssh_con
		# Conn Type: SHH
		# Host: localhost
		# Password: ********
		# Save diyoruz.
		t3 = SSHOperator(task_id = "clean_dirty_data", ssh_conn_id = 'my_ssh_con', command = 'source /home/train/venvspark/bin/activate; spark-submit --master-local /home/train/pythonProject/dags/scripts/spark_dirty_data_cleaner.py')
		# t4 taskı için bir connection belirlememiz lazım. Airflow arayüzünden Admin>>Connection diyerek '+' işaretine basarak yeni bir connection belirleyelim.
		# Conn Id: my_postgresql_conn
		# Conn Type: Postgres
		# Host: localhost
		# Schema: traindb
		# Login: train
		# Password: ********
		# Port: ****
		# Save diyoruz.
		t4 = PostgresOperator(task_id = 'create_table', postgres_conn_id = 'my_postgresql_conn', sql = 'create_clean_trns_limited.sql')
		t1 >> t2 >> t3 >> t4
    
    
(venvairflow) [train@localhost pythonProject]$ cp -r dags/ ~/venvairflow/
(venvairflow) [train@localhost pythonProject]$ airflow dags test simple_spark_sql '2021-10-25'
(venvairflow) [train@localhost pythonProject]$ /usr/pgsql-10/bin/pg_dump -d traindb -U train --table-clean-transactions --schema-only
CREATE TABLE public.clean_transactions (
		"STORE_ID" text,
		"STORE_LOCATION" text,
		"PRODUCT_CATEGORY" text,
		"PRODUCT_ID" text,
		"MPR" real,
		"CP" real,
		"DISCOUNT" real,
		"SP" real,
		"sale_date date
);

DROP TABLE public.clean_trans_limited;
CREATE TABLE IF NOT EXISTS public.clean_trans_limited (
		"STORE_LOCATION" text,
		"PRODUCT_CATEGORY" text,
		"PRODUCT_ID" text,
		"MPR" real,
		"CP" real,
		"DISCOUNT" real,
		"SP" real,
		"sale_date date
);

(venvairflow) [train@localhost pythonProject]$ psql -d traindb;
traindb=> CREATE TABLE public.clean_trans_limited (
							"STORE_LOCATION" text,
							"PRODUCT_CATEGORY" text,
							"PRODUCT_ID" text,
							"MPR" real,
							"CP" real,
							"DISCOUNT" real,
							"SP" real,
							"sale_date date
);
traindb=> dt\
traindb=> select * from clean_trans_limited limit 5;
# Sorgu çalışıyor.

(venvairflow) [train@localhost pythonProject]$ cp -r dags/ ~/venvairflow/
(venvairflow) [train@localhost pythonProject]$ airflow dags test simple_spark_sql '2021-10-25'

