"""
Order Analytics DAG

Orchestrates the order analytics ETL pipeline, processing order volumes,
revenue metrics, and product trends from the orders service.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
)
from acme_shop_analytics_etl.etl.order_analytics_job import (
    extract_order_data,
    extract_order_data_legacy,
    transform_order_metrics,
    transform_order_metrics_v1,
    load_order_analytics,
)
from acme_shop_analytics_etl.logging.structured_logging import get_logger

logger = get_logger(__name__)

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
    "order_analytics",
    default_args=default_args,
    description="Process order analytics data from orders service",
    schedule_interval="0 3 * * *",  # Run daily at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "orders", "etl", "revenue"],
)


def choose_extraction_path(**context):
    """Branch based on feature flags."""
    if is_legacy_etl_enabled():
        # TODO(TEAM-PLATFORM): Remove legacy branch after migration
        logging.info("Routing to legacy extraction path")
        return "extract_orders_legacy"
    
    logger.info("Routing to v2 extraction path", extra={"branch": "v2"})
    return "extract_orders_v2"


def run_extract_legacy(**context):
    """Extract order data using legacy pipeline."""
    execution_date = context["execution_date"]
    logging.info(f"Starting legacy order extraction for {execution_date}")
    
    return extract_order_data_legacy(
        start_date=execution_date - timedelta(days=1),
        end_date=execution_date,
    )


def run_extract_v2(**context):
    """Extract order data using v2 pipeline."""
    execution_date = context["execution_date"]
    logger.info(
        "Starting v2 order extraction",
        extra={
            "execution_date": execution_date.isoformat(),
            "pipeline_version": "v2",
        },
    )
    
    return extract_order_data(
        start_date=execution_date - timedelta(days=1),
        end_date=execution_date,
    )


def choose_transform_path(**context):
    """Branch based on schema version."""
    if is_v1_schema_enabled():
        logging.info("Routing to v1 schema transformation")
        return "transform_orders_v1"
    
    logger.info("Routing to v2 schema transformation", extra={"schema": "v2"})
    return "transform_orders_v2"


def run_transform_v1(**context):
    """Transform using v1 schema (legacy)."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids=["extract_orders_legacy", "extract_orders_v2"])
    data = next((d for d in raw_data if d is not None), [])
    
    # TODO(TEAM-API): V1 schema has known issues with decimal precision
    logging.info(f"Transforming {len(data)} orders with v1 schema")
    
    return transform_order_metrics_v1(data)


def run_transform_v2(**context):
    """Transform using v2 schema."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids=["extract_orders_legacy", "extract_orders_v2"])
    data = next((d for d in raw_data if d is not None), [])
    
    logger.info(
        "Transforming orders with v2 schema",
        extra={"record_count": len(data), "schema": "v2"},
    )
    
    return transform_order_metrics(data)


def run_load(**context):
    """Load transformed metrics into analytics warehouse."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids=["transform_orders_v1", "transform_orders_v2"])
    data = next((m for m in metrics if m is not None), [])
    
    logger.info(
        "Loading order analytics",
        extra={"metric_count": len(data)},
    )
    
    load_order_analytics(data)
    logger.info("Order analytics load complete", extra={"status": "success"})


branch_extract = BranchPythonOperator(
    task_id="branch_extraction",
    python_callable=choose_extraction_path,
    dag=dag,
)

extract_legacy = PythonOperator(
    task_id="extract_orders_legacy",
    python_callable=run_extract_legacy,
    dag=dag,
)

extract_v2 = PythonOperator(
    task_id="extract_orders_v2",
    python_callable=run_extract_v2,
    dag=dag,
)

branch_transform = BranchPythonOperator(
    task_id="branch_transformation",
    python_callable=choose_transform_path,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

transform_v1 = PythonOperator(
    task_id="transform_orders_v1",
    python_callable=run_transform_v1,
    dag=dag,
)

transform_v2 = PythonOperator(
    task_id="transform_orders_v2",
    python_callable=run_transform_v2,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_orders",
    python_callable=run_load,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

branch_extract >> [extract_legacy, extract_v2] >> branch_transform
branch_transform >> [transform_v1, transform_v2] >> load_task
