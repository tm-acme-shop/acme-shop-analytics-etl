"""
Order Analytics ETL Job

Processes order data for analytics including revenue metrics,
order volumes, and product trends.
"""
import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
)
from acme_shop_analytics_etl.db.queries import fetch_order_analytics, insert_analytics_batch
from acme_shop_analytics_etl.db.legacy_queries import get_orders_by_user_id_legacy
from acme_shop_analytics_etl.etl.common import ETLResult, batch_records
from acme_shop_analytics_etl.etl.deduplication import RecordDeduplicator
from acme_shop_analytics_etl.logging.structured_logging import get_logger
from acme_shop_analytics_etl.models import get_order_model

logger = get_logger(__name__)


def extract_order_data(
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Extract order data from source database.
    
    Args:
        start_date: Start of extraction window.
        end_date: End of extraction window.
    
    Returns:
        List of order records.
    """
    logger.info(
        "Extracting order data",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    
    return fetch_order_analytics(start_date=start_date, end_date=end_date)


def extract_order_data_legacy(
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Extract order data using legacy pipeline.
    
    DEPRECATED: Uses unsafe SQL patterns.
    TODO(TEAM-PLATFORM): Remove after v2 migration
    
    Args:
        start_date: Start date.
        end_date: End date.
    
    Returns:
        List of order records.
    """
    # TODO(TEAM-PLATFORM): Legacy extraction - remove after migration
    logging.info(f"Running legacy order extraction for {start_date} to {end_date}")
    
    # This is a placeholder - in real code it would query orders
    return []


def transform_order_metrics(
    raw_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Transform raw order data into analytics metrics (v2).
    
    Args:
        raw_data: List of raw order records.
    
    Returns:
        List of transformed metric records.
    """
    if not raw_data:
        logger.info("No order data to transform", extra={"record_count": 0})
        return []
    
    logger.info(
        "Transforming order metrics (v2)",
        extra={"record_count": len(raw_data)},
    )
    
    # Deduplicate
    deduplicator = RecordDeduplicator(use_legacy_hash=False)
    unique_records = deduplicator.deduplicate_batch(raw_data)
    
    metrics = []
    for record in unique_records:
        try:
            metric = {
                "order_date": record.get("order_date"),
                "status": record.get("status"),
                "order_count": record.get("order_count", 0),
                "total_revenue": str(Decimal(str(record.get("total_revenue", 0)))),
                "avg_order_value": str(Decimal(str(record.get("avg_order_value", 0)))),
            }
            metrics.append(metric)
        except Exception as e:
            logger.warning(
                "Failed to transform order record",
                extra={"error": str(e)},
            )
    
    logger.info(
        "Order transformation complete",
        extra={
            "input_count": len(raw_data),
            "output_count": len(metrics),
        },
    )
    
    return metrics


def transform_order_metrics_v1(
    raw_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Transform raw order data using v1 schema (legacy).
    
    DEPRECATED: V1 uses float for currency which causes precision issues.
    TODO(TEAM-API): Remove after v2 migration
    
    Args:
        raw_data: List of raw order records.
    
    Returns:
        List of transformed metric records.
    """
    if not raw_data:
        return []
    
    # TODO(TEAM-API): V1 uses float for currency - precision issues
    logging.info("Using v1 order transformation (deprecated)")
    
    # Deduplicate using legacy MD5
    deduplicator = RecordDeduplicator(use_legacy_hash=True)
    unique_records = deduplicator.deduplicate_batch(raw_data)
    
    metrics = []
    for record in unique_records:
        try:
            # Using float instead of Decimal (anti-pattern)
            metric = {
                "order_date": record.get("order_date"),
                "status": record.get("status"),
                "order_count": record.get("order_count", 0),
                "total_revenue": float(record.get("total_revenue", 0)),  # TODO: Use Decimal
                "avg_order_value": float(record.get("avg_order_value", 0)),  # TODO: Use Decimal
            }
            metrics.append(metric)
        except Exception as e:
            logging.warning(f"Failed to transform order: {e}")
    
    return metrics


def load_order_analytics(metrics: List[Dict[str, Any]], dry_run: bool = False) -> int:
    """
    Load transformed order metrics into analytics warehouse.
    
    Args:
        metrics: List of metric records to load.
        dry_run: If True, skip actual database writes.
    
    Returns:
        Number of records loaded.
    """
    if not metrics:
        logger.info("No order metrics to load", extra={"record_count": 0})
        return 0
    
    logger.info(
        "Loading order analytics",
        extra={
            "record_count": len(metrics),
            "dry_run": dry_run,
        },
    )
    
    if dry_run:
        logger.info("Dry run - skipping database load")
        return len(metrics)
    
    loaded = 0
    for batch in batch_records(metrics):
        try:
            inserted = insert_analytics_batch("order_analytics", batch)
            loaded += inserted
        except Exception as e:
            logger.error(
                "Failed to load order analytics batch",
                extra={"error": str(e)},
            )
    
    logger.info(
        "Order analytics load complete",
        extra={"loaded": loaded},
    )
    
    return loaded


def run_order_analytics_etl(
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run the complete order analytics ETL job.
    
    Args:
        start_date: Start of data extraction window.
        end_date: End of data extraction window.
        dry_run: If True, skip database writes.
    
    Returns:
        ETL result dictionary.
    """
    result = ETLResult(job_name="order_analytics")
    result.start_time = datetime.now()
    
    logger.info(
        "Starting order analytics ETL",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "dry_run": dry_run,
            "legacy_enabled": is_legacy_etl_enabled(),
            "v1_schema": is_v1_schema_enabled(),
        },
    )
    
    try:
        # Extract
        if is_legacy_etl_enabled():
            raw_data = extract_order_data_legacy(start_date, end_date)
        else:
            raw_data = extract_order_data(start_date, end_date)
        result.records_extracted = len(raw_data)
        
        # Transform
        if is_v1_schema_enabled():
            metrics = transform_order_metrics_v1(raw_data)
        else:
            metrics = transform_order_metrics(raw_data)
        result.records_transformed = len(metrics)
        
        # Load
        loaded = load_order_analytics(metrics, dry_run=dry_run)
        result.records_loaded = loaded
        result.records_processed = loaded
        
        result.status = "success"
        
    except Exception as e:
        result.status = "failed"
        result.errors.append(str(e))
        logger.error(
            "Order analytics ETL failed",
            extra={"error": str(e)},
            exc_info=True,
        )
    
    result.end_time = datetime.now()
    result.duration_seconds = (result.end_time - result.start_time).total_seconds()
    
    logger.info(
        "Order analytics ETL complete",
        extra=result.to_dict(),
    )
    
    return result.to_dict()
