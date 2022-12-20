import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import vertica_python
import logging
import os
import sys

dag_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, dag_folder)

def update_mart(conn_id, update_date):
    db_conn = BaseHook.get_connection(conn_id)
    task_logger = logging.getLogger('airflow.task')
    task_logger.info(f"Date to be updated: {update_date}")
    conn_info = db_conn.extra_dejson
    with open (dag_folder+"/sql/mart_query_daily.sql", mode="r") as f:
        sql = f.read()
        sql = sql.format(execution_date="'"+str(update_date)+"'::timestamp")    
    task_logger.info(f"Try to execute merge query")
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cur.execute("commit")
    task_logger.info(f"Metrics for {update_date} were succesfully updates")

with DAG(
        dag_id = '2_datamart_update',
        schedule_interval='@daily',
        start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
        end_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
        catchup=True,
        tags=['final project']
) as dag:
           update_mart = PythonOperator(
            task_id='update_mart',
            python_callable = update_mart,
            op_kwargs = {
            "conn_id": 'VERTICA_CONN',
            "update_date": '{{ds}}'
            }
            )