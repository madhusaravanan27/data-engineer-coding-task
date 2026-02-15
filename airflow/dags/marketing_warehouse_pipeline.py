from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

DBT_DIR = "/opt/airflow/dbt/campaignwarehouse"
INGEST_DIR = "/opt/airflow/ingestion"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketing_warehouse_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  
    catchup=False,
    max_active_runs=1,      
    tags=["marketing", "warehouse", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    

    ingest_google = BashOperator(
        task_id="ingest_google",
        bash_command=f"python {INGEST_DIR}/load_google.py",
    )

    ingest_facebook = BashOperator(
        task_id="ingest_facebook",
        bash_command=f"python {INGEST_DIR}/load_facebook.py",
    )

    ingest_crm = BashOperator(
        task_id="ingest_crm",
        bash_command=f"python {INGEST_DIR}/load_crm.py",
    )

  
    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
    cd /opt/airflow/dbt/campaignwarehouse &&
    dbt run --profiles-dir /opt/airflow/dbt
    """,
    )


    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
    cd /opt/airflow/dbt/campaignwarehouse &&
    dbt test --profiles-dir /opt/airflow/dbt
    """,
    )


    end = EmptyOperator(task_id="end")


    start >> [ingest_google, ingest_facebook, ingest_crm] >> dbt_run >> dbt_test >> end
