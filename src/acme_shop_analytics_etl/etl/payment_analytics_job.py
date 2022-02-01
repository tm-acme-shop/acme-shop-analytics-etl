"""
Payment Analytics ETL Job

Processes payment data for analytics including success rates,
fraud detection metrics, and revenue tracking.
"""
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_legacy_pii_enabled,
)
from acme_shop_analytics_etl.db.queries import fetch_payment_analytics, insert_analytics_batch
from acme_shop_analytics_etl.etl.common import ETLResult, batch_records
from acme_shop_analytics_etl.etl.deduplication import RecordDeduplicator
from acme_shop_analytics_etl.logging.structured_logging import get_logger
from acme_shop_analytics_etl.pii.handlers import tokenize_payment_info
from acme_shop_analytics_etl.pii.legacy_pii import mask_card_number_legacy

logger = get_logger(__name__)


def extract_payment_data(
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Extract payment data from source database.
    
    Args:
        start_date: Start of extraction window.
        end_date: End of extraction window.
    
    Returns:
        List of payment records.
    """
    logger.info(
        "Extracting payment data",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    
    return fetch_payment_analytics(start_date=start_date, end_date=end_date)


def run_legacy_payment_etl(execution_date: datetime) -> List[Dict[str, Any]]:
    """
    Run legacy payment ETL pipeline.
    
    DEPRECATED: Uses unsafe PII handling patterns.
    TODO(TEAM-SEC): Remove after migration to v2 pipeline
    
    Args:
        execution_date: Date for ETL run.
    
    Returns:
        List of extracted payment records.
    """
    # TODO(TEAM-SEC): Legacy extraction with PII risks
    logging.info(f"Running legacy payment ETL for {execution_date}")
    
    # Placeholder - actual implementation would query database
    return []


def transform_payment_metrics(
    raw_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Transform raw payment data into analytics metrics.
    
    Handles PII tokenization based on feature flags.
    
    Args:
        raw_data: List of raw payment records.
    
    Returns:
        List of transformed metric records.
    """
    if not raw_data:
        logger.info("No payment data to transform", extra={"record_count": 0})
        return []
    
    logger.info(
        "Transforming payment metrics",
        extra={
            "record_count": len(raw_data),
            "legacy_pii": is_legacy_pii_enabled(),
        },
    )
    
    # Deduplicate
    deduplicator = RecordDeduplicator(use_legacy_hash=is_legacy_etl_enabled())
    unique_records = deduplicator.deduplicate_batch(raw_data)
    
    metrics = []
    for record in unique_records:
        try:
            # Handle PII based on feature flag
            if is_legacy_pii_enabled():
                # TODO(TEAM-SEC): Legacy PII masking is insufficient
                processed = _process_payment_legacy(record)
            else:
                processed = _process_payment_v2(record)
            
            metric = {
                "payment_date": processed.get("payment_date"),
                "payment_method": processed.get("payment_method"),
                "transaction_count": processed.get("transaction_count", 0),
                "total_amount": str(Decimal(str(processed.get("total_amount", 0)))),
                "successful_count": processed.get("successful", 0),
                "failed_count": processed.get("failed", 0),
                "success_rate": _calculate_success_rate(
                    processed.get("successful", 0),
                    processed.get("transaction_count", 0),
                ),
                "avg_processing_time_ms": processed.get("avg_processing_time"),
            }
            metrics.append(metric)
            
        except Exception as e:
            logger.warning(
                "Failed to transform payment record",
                extra={"error": str(e)},
            )
    
    logger.info(
        "Payment transformation complete",
        extra={
            "input_count": len(raw_data),
            "output_count": len(metrics),
        },
    )
    
    return metrics


def _process_payment_legacy(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process payment with legacy PII handling.
    
    DEPRECATED: Uses masking instead of tokenization.
    TODO(TEAM-SEC): Remove after PII migration
    """
    # TODO(TEAM-SEC): Masking is not sufficient for PII protection
    logging.info("Using legacy PII masking for payment")
    
    processed = record.copy()
    
    if "card_number" in processed and processed["card_number"]:
        processed["card_display"] = mask_card_number_legacy(processed["card_number"])
        # Note: Legacy code keeps the original - this is a security issue!
    
    return processed


def _process_payment_v2(record: Dict[str, Any]) -> Dict[str, Any]:
    """Process payment with v2 tokenization."""
    return tokenize_payment_info(record)


def _calculate_success_rate(successful: int, total: int) -> float:
    """Calculate payment success rate."""
    if total <= 0:
        return 0.0
    return round(successful / total * 100, 2)


def load_payment_analytics(metrics: List[Dict[str, Any]], dry_run: bool = False) -> int:
    """
    Load transformed payment metrics into analytics warehouse.
    
    Args:
        metrics: List of metric records to load.
        dry_run: If True, skip actual database writes.
    
    Returns:
        Number of records loaded.
    """
    if not metrics:
        logger.info("No payment metrics to load", extra={"record_count": 0})
        return 0
    
    logger.info(
        "Loading payment analytics",
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
            inserted = insert_analytics_batch("payment_analytics", batch)
            loaded += inserted
        except Exception as e:
            logger.error(
                "Failed to load payment analytics batch",
                extra={"error": str(e)},
            )
    
    logger.info(
        "Payment analytics load complete",
        extra={"loaded": loaded},
    )
    
    return loaded


def run_payment_analytics_etl(
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run the complete payment analytics ETL job.
    
    Args:
        start_date: Start of data extraction window.
        end_date: End of data extraction window.
        dry_run: If True, skip database writes.
    
    Returns:
        ETL result dictionary.
    """
    result = ETLResult(job_name="payment_analytics")
    result.start_time = datetime.now()
    
    logger.info(
        "Starting payment analytics ETL",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "dry_run": dry_run,
            "legacy_pii_enabled": is_legacy_pii_enabled(),
        },
    )
    
    try:
        # Extract
        if is_legacy_etl_enabled():
            raw_data = run_legacy_payment_etl(start_date)
        else:
            raw_data = extract_payment_data(start_date, end_date)
        result.records_extracted = len(raw_data)
        
        # Transform
        metrics = transform_payment_metrics(raw_data)
        result.records_transformed = len(metrics)
        
        # Load
        loaded = load_payment_analytics(metrics, dry_run=dry_run)
        result.records_loaded = loaded
        result.records_processed = loaded
        
        result.status = "success"
        
    except Exception as e:
        result.status = "failed"
        result.errors.append(str(e))
        logger.error(
            "Payment analytics ETL failed",
            extra={"error": str(e)},
            exc_info=True,
        )
    
    result.end_time = datetime.now()
    result.duration_seconds = (result.end_time - result.start_time).total_seconds()
    
    logger.info(
        "Payment analytics ETL complete",
        extra=result.to_dict(),
    )
    
    return result.to_dict()
