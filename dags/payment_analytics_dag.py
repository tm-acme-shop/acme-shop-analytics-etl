"""
Payment Analytics DAG

Orchestrates the payment analytics ETL pipeline, processing payment success rates,
fraud detection metrics, and revenue tracking from the payments service.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from acme_shop_analytics_etl.config.feature_flags import is_legacy_etl_enabled
from acme_shop_analytics_etl.etl.payment_analytics_job import (
    extract_payment_data,
    transform_payment_metrics,
    load_payment_analytics,
    run_legacy_payment_etl,
)
from acme_shop_analytics_etl.pii.legacy_pii import mask_card_number_legacy
from acme_shop_analytics_etl.pii.handlers import tokenize_payment_info
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
    "payment_analytics",
    default_args=default_args,
    description="Process payment analytics from payments service",
    schedule_interval="0 4 * * *",  # Run daily at 4 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "payments", "etl", "pci"],
)


def run_extract(**context):
    """Extract payment data from source systems."""
    execution_date = context["execution_date"]
    
    if is_legacy_etl_enabled():
        # TODO(TEAM-SEC): Legacy extraction has PII exposure risks
        logging.info(f"Using legacy payment extraction for {execution_date}")
        return run_legacy_payment_etl(execution_date)
    
    logger.info(
        "Starting payment extraction",
        extra={
            "execution_date": execution_date.isoformat(),
            "pipeline": "v2",
        },
    )
    
    return extract_payment_data(
        start_date=execution_date - timedelta(days=1),
        end_date=execution_date,
    )


def run_pii_handling(**context):
    """Handle PII in payment records."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_payments")
    
    if not raw_data:
        logger.info("No payment data to process", extra={"record_count": 0})
        return []
    
    processed = []
    for record in raw_data:
        if is_legacy_etl_enabled():
            # TODO(TEAM-SEC): Replace legacy PII handling with tokenization
            logging.info("Using legacy PII masking")
            record["card_display"] = mask_card_number_legacy(record.get("card_number", ""))
        else:
            logger.info(
                "Tokenizing payment PII",
                extra={"record_id": record.get("id")},
            )
            record = tokenize_payment_info(record)
        
        processed.append(record)
    
    logger.info(
        "PII handling complete",
        extra={"processed_count": len(processed)},
    )
    return processed


def run_transform(**context):
    """Transform payment data into analytics metrics."""
    ti = context["ti"]
    pii_handled_data = ti.xcom_pull(task_ids="handle_pii")
    
    logger.info(
        "Transforming payment metrics",
        extra={"record_count": len(pii_handled_data) if pii_handled_data else 0},
    )
    
    return transform_payment_metrics(pii_handled_data or [])


def run_load(**context):
    """Load transformed payment metrics into analytics warehouse."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="transform_payments")
    
    logger.info(
        "Loading payment analytics",
        extra={"metric_count": len(metrics) if metrics else 0},
    )
    
    load_payment_analytics(metrics or [])
    logger.info("Payment analytics load complete", extra={"status": "success"})


extract_task = PythonOperator(
    task_id="extract_payments",
    python_callable=run_extract,
    dag=dag,
)

pii_task = PythonOperator(
    task_id="handle_pii",
    python_callable=run_pii_handling,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_payments",
    python_callable=run_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_payments",
    python_callable=run_load,
    dag=dag,
)

extract_task >> pii_task >> transform_task >> load_task
