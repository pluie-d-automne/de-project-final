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

def update_stage(kafka_conn_id, conn_id):
    from py.spark_func import (spark_init,
                               load_df_kafka,
                               transform,
                               create_foreach_batch_function,
                               spark_jars_packages)
    task_logger = logging.getLogger('airflow.task')
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    kafka_security_options = kafka_conn.extra_dejson
    db_conn = BaseHook.get_connection(conn_id)
    conn_info = db_conn.extra_dejson
    vertica_url = f'jdbc:vertica://{conn_info["host"]}:{conn_info["port"]}/dwh'
    vertica_settings = {
    'user': conn_info["user"],
    'password': conn_info["password"],
    'driver': 'com.vertica.jdbc.Driver',
    }
    spark = spark_init()
    df = load_df_kafka(spark,
                       topic='transaction-service-input',
                       kafka_security_options=kafka_security_options)
    df = transform(df)
    foreach_batch_function = create_foreach_batch_function(vertica_url=vertica_url, vertica_settings=vertica_settings)
    query = (df.writeStream\
            .foreachBatch(foreach_batch_function)
            .start()
            .awaitTermination(120) # Ожидание 2 минуты
            )

    

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
    update_stage = PythonOperator(
            task_id='update_stage',
            python_callable = update_stage,
            op_kwargs = {
            "conn_id": 'VERTICA_CONN',
            "kafka_conn_id": 'KAFKA_CONN'
            }
            ),
           
    update_mart = PythonOperator(
            task_id='update_mart',
            python_callable = update_mart,
            op_kwargs = {
            "conn_id": 'VERTICA_CONN',
            "update_date": '{{ds}}'
            }
            )
           
    update_stage >> update_mart