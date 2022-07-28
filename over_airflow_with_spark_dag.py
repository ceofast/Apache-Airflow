from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%y-%m-%d')

print(yesterday_date)

default_args = {
		'owner':'train',
		'start_date': yesterday_date,
		'retries':1,
		'retry_delay': timedelta(seconds=5)
}

with DAG('simple_spark_dag', default_args = default_args, schedule_interval = '@daily', catchup = False) as dag:

		t1 = BashOperator(task_id = 'download_data', bash_command = 'wget https://github.com/erkansirin78/datasets/raw/master/dirty_store_transactions.csv -O /tmp/dirty_store_transactions.csv')

		t2 = FileSensor(task_id = 'check_file_exists', filepath = '/tmp/dirty_store_transactions.csv')
		# ssh_conn_id için airflow arayüzünü açıyoruz, Admin'i tıklayıp connections'a geliyoruz, + işaretini tıklayarak kendimize ssh_connector yaratıyoruz.
		# Conn Id: my_ssh_con
		# Conn Type: SHH
		# Host: localhost
		# Password: ********
		# Save diyoruz.
		t3 = SSHOperator(task_id = "clean_dirty_data", ssh_conn_id = 'my_ssh_con', command = 'source /home/train/venvspark/bin/activate; spark-submit --master-local /home/train/pythonProject/dags/scripts/spark_dirty_data_cleaner.py')


		t1 >> t2 >> t3
    
    (venvairflow) [train@localhost pythonProject]$ cp dags/ ~/venvairflow/
(venvairflow) [train@localhost pythonProject]$ airflow dags test simple_spark_dag '2021-10-25'
(venvairflow) [train@localhost pythonProject]$ psql -U train -d traindb
psql (9.2.24, server 10.17)
WARNING: psql version 9.2, server version 10.0.
				 Some psql features might not work.
Type 'help' for helps.

traindb=> \dt

traindb=> drop table clean_transaction;
DROP TABLE
traindb=> select * from clean_transactions limit 5;

traindb=> select * from clean_transactions limit 5;
# Tablo bu sefer geldi.
