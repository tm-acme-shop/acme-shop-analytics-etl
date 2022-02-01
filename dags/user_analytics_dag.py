"""
User Analytics DAG

Orchestrates the user analytics ETL pipeline, processing user registration,
activity, and churn metrics from the users service.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
)
from acme_shop_analytics_etl.etl.user_analytics_job import (
    extract_user_data,
    transform_user_metrics,
    load_user_analytics,
    run_legacy_user_etl,
)

# TODO(TEAM-PLATFORM): Migrate to structured logging across all DAGs
logging.info("Initializing user_analytics_dag")

default_args = {
    "owner": "analytics-team",
    "depends_on_past": False,
    "email": ["analytics-alerts@acmeshop.example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "user_analytics",
    default_args=default_args,
    description="Process user analytics data from users service",
    schedule_interval="0 2 * * *",  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "users", "etl"],
)


def run_extract(**context):
    """Extract user data from source systems."""
    execution_date = context["execution_date"]
    # TODO(TEAM-API): Migrate from legacy logging to structured
    logging.info(f"Starting user data extraction for {execution_date}")
    
    if is_legacy_etl_enabled():
        logging.info("Using legacy extraction pipeline")
        return run_legacy_user_etl(execution_date)
    
    return extract_user_data(
        start_date=execution_date - timedelta(days=1),
        end_date=execution_date,
    )


def run_transform(**context):
    """Transform extracted user data into analytics metrics."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_users")
    
    logging.info(f"Transforming {len(raw_data) if raw_data else 0} user records")
    
    use_v1 = is_v1_schema_enabled()
    # TODO(TEAM-PLATFORM): Remove v1 schema support after migration
    logging.info(f"Using {'v1' if use_v1 else 'v2'} schema for transformation")
    
    return transform_user_metrics(raw_data, use_legacy_schema=use_v1)


def run_load(**context):
    """Load transformed metrics into analytics warehouse."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="transform_users")
    
    logging.info(f"Loading {len(metrics) if metrics else 0} metric records")
    
    load_user_analytics(metrics)
    logging.info("User analytics load complete")


extract_task = PythonOperator(
    task_id="extract_users",
    python_callable=run_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_users",
    python_callable=run_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_users",
    python_callable=run_load,
    dag=dag,
)

extract_task >> transform_task >> load_task
