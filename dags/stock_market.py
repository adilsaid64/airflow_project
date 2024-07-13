from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files  import File
from astro.sql.table import Table, Metadata

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME
SYMBOL = 'AAPL'

@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock_market'] # can do tags by teams or projects
)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')  # allows you to wait for an event.
    def is_api_available() -> PokeReturnValue:
        # verify that the API is available.
        api = BaseHook.get_connection('stock_api')  # An interface to interact with an outside service.
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])

        condition = response.json()['finance']['result'] is None # creates a boolean condition
        return PokeReturnValue(is_done=condition, xcom_value=url)

    # why using python operator here instead of task operator? Because later on we will be using the docker operator. best practice is to avoid mixing operators with doceratores.
    # python operator executes a python function.
    get_stock_prices = PythonOperator( 
        task_id = 'get_stock_prices',
        python_callable= _get_stock_prices,
        op_kwargs={'url' :'{{task_instance.xcom_pull(task_ids = "is_api_available")}}', # tompulate airflow concept.  Allows you to inject data when dag is running. xcom_value from is_api_avaliable. two curly brackets is whenn somethhing is replaced.
                   'symbol':SYMBOL} 
    )


    stock_prices = PythonOperator(
        task_id= 'store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock':'{{task_instance.xcom_pull(task_ids = "get_stock_prices")}}'}
    )


    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',  # Ensure spark-master is the correct container name
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }  # Ensure stock_prices task returns the path of where the JSON is stored in S3
    )

    get_formatted_csv = PythonOperator(
        task_id = 'get_formatted_csv', 
        python_callable= _get_formatted_csv,
        op_kwargs = {
            'path' : '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
        
    )


    load_to_dtw = aql.load_file(
        task_id='load_to_dtw',
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{task_instance.xcom_pull(task_ids='store_prices')}}",
            conn_id='minio'
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        )
    )


    is_api_available() >> get_stock_prices >> stock_prices >> format_prices >> get_formatted_csv >> load_to_dtw

stock_market()
