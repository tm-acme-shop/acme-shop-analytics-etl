"""
Notification Analytics DAG

Orchestrates the notification analytics ETL pipeline, processing delivery rates,
engagement metrics, and channel performance from the notifications service.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from acme_shop_analytics_etl.config.feature_flags import is_v1_schema_enabled
from acme_shop_analytics_etl.etl.notification_analytics_job import (
    extract_notification_data,
    transform_notification_metrics,
    load_notification_analytics,
    calculate_channel_metrics,
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
    "notification_analytics",
    default_args=default_args,
    description="Process notification analytics from notifications service",
    schedule_interval="0 5 * * *",  # Run daily at 5 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytics", "notifications", "etl", "engagement"],
)


def run_extract(**context):
    """Extract notification data from source systems."""
    execution_date = context["execution_date"]
    request_id = context.get("run_id", "unknown")
    
    logger.info(
        "Starting notification extraction",
        extra={
            "execution_date": execution_date.isoformat(),
            "request_id": request_id,
            "x_acme_request_id": request_id,  # Correlation header
        },
    )
    
    return extract_notification_data(
        start_date=execution_date - timedelta(days=1),
        end_date=execution_date,
    )


def run_transform(**context):
    """Transform notification data into analytics metrics."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_notifications")
    request_id = context.get("run_id", "unknown")
    
    record_count = len(raw_data) if raw_data else 0
    logger.info(
        "Transforming notification metrics",
        extra={
            "record_count": record_count,
            "request_id": request_id,
            "x_acme_request_id": request_id,
        },
    )
    
    use_v1 = is_v1_schema_enabled()
    # TODO(TEAM-API): V1 schema doesn't support push notification metrics
    if use_v1:
        logger.info(
            "Using v1 schema - push metrics will be limited",
            extra={"schema_version": "v1"},
        )
    
    return transform_notification_metrics(raw_data or [], use_legacy_schema=use_v1)


def run_channel_metrics(**context):
    """Calculate per-channel performance metrics."""
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_notifications")
    request_id = context.get("run_id", "unknown")
    
    logger.info(
        "Calculating channel metrics",
        extra={
            "input_count": len(transformed_data) if transformed_data else 0,
            "request_id": request_id,
            "x_acme_request_id": request_id,
        },
    )
    
    channel_metrics = calculate_channel_metrics(transformed_data or [])
    
    for channel, metrics in channel_metrics.items():
        logger.info(
            "Channel metrics calculated",
            extra={
                "channel": channel,
                "delivery_rate": metrics.get("delivery_rate"),
                "open_rate": metrics.get("open_rate"),
                "click_rate": metrics.get("click_rate"),
                "request_id": request_id,
            },
        )
    
    return channel_metrics


def run_load(**context):
    """Load transformed notification metrics into analytics warehouse."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="transform_notifications")
    channel_metrics = ti.xcom_pull(task_ids="calculate_channel_metrics")
    request_id = context.get("run_id", "unknown")
    
    logger.info(
        "Loading notification analytics",
        extra={
            "metric_count": len(metrics) if metrics else 0,
            "channel_count": len(channel_metrics) if channel_metrics else 0,
            "request_id": request_id,
            "x_acme_request_id": request_id,
        },
    )
    
    load_notification_analytics(
        notification_metrics=metrics or [],
        channel_metrics=channel_metrics or {},
    )
    
    logger.info(
        "Notification analytics load complete",
        extra={
            "status": "success",
            "request_id": request_id,
        },
    )


extract_task = PythonOperator(
    task_id="extract_notifications",
    python_callable=run_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_notifications",
    python_callable=run_transform,
    dag=dag,
)

channel_metrics_task = PythonOperator(
    task_id="calculate_channel_metrics",
    python_callable=run_channel_metrics,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_notifications",
    python_callable=run_load,
    dag=dag,
)

extract_task >> transform_task >> [channel_metrics_task, load_task]
channel_metrics_task >> load_task
