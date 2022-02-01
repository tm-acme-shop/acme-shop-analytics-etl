"""
User Analytics ETL Job

Processes user data for analytics including registration metrics,
activity patterns, and churn analysis.
"""
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
)
from acme_shop_analytics_etl.db.queries import fetch_user_analytics, insert_analytics_batch
from acme_shop_analytics_etl.db.legacy_queries import get_users_by_date_range_legacy
from acme_shop_analytics_etl.etl.common import ETLResult, batch_records
from acme_shop_analytics_etl.etl.deduplication import RecordDeduplicator
from acme_shop_analytics_etl.logging.structured_logging import get_logger
from acme_shop_analytics_etl.models import get_user_model

logger = get_logger(__name__)


def extract_user_data(
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Extract user data from source database.
    
    Args:
        start_date: Start of extraction window.
        end_date: End of extraction window.
    
    Returns:
        List of user records.
    """
    logger.info(
        "Extracting user data",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    
    return fetch_user_analytics(
        start_date=start_date,
        end_date=end_date,
        use_v2_schema=not is_v1_schema_enabled(),
    )


def run_legacy_user_etl(execution_date: datetime) -> List[Dict[str, Any]]:
    """
    Run legacy user ETL pipeline.
    
    DEPRECATED: Uses legacy extraction with unsafe SQL patterns.
    TODO(TEAM-PLATFORM): Remove after migration to v2 pipeline
    
    Args:
        execution_date: Date for ETL run.
    
    Returns:
        List of extracted user records.
    """
    # TODO(TEAM-PLATFORM): This uses unsafe SQL patterns
    logging.info(f"Running legacy user ETL for {execution_date}")
    
    start_date = execution_date.strftime("%Y-%m-%d")
    end_date = (execution_date).strftime("%Y-%m-%d")
    
    # Uses string interpolation SQL - unsafe!
    return get_users_by_date_range_legacy(start_date, end_date)


def transform_user_metrics(
    raw_data: List[Dict[str, Any]],
    use_legacy_schema: bool = False,
) -> List[Dict[str, Any]]:
    """
    Transform raw user data into analytics metrics.
    
    Args:
        raw_data: List of raw user records.
        use_legacy_schema: If True, use v1 transformations.
    
    Returns:
        List of transformed metric records.
    """
    if not raw_data:
        logger.info("No data to transform", extra={"record_count": 0})
        return []
    
    logger.info(
        "Transforming user metrics",
        extra={
            "record_count": len(raw_data),
            "schema": "v1" if use_legacy_schema else "v2",
        },
    )
    
    # Deduplicate records
    deduplicator = RecordDeduplicator(use_legacy_hash=is_legacy_etl_enabled())
    unique_records = deduplicator.deduplicate_batch(raw_data)
    
    metrics = []
    UserModel = get_user_model()
    
    for record in unique_records:
        try:
            # Transform based on schema version
            if use_legacy_schema:
                metric = _transform_user_v1(record)
            else:
                metric = _transform_user_v2(record)
            
            if metric:
                metrics.append(metric)
                
        except Exception as e:
            logger.warning(
                "Failed to transform user record",
                extra={
                    "record_id": record.get("id"),
                    "error": str(e),
                },
            )
    
    logger.info(
        "User transformation complete",
        extra={
            "input_count": len(raw_data),
            "output_count": len(metrics),
            "deduplicated": len(raw_data) - len(unique_records),
        },
    )
    
    return metrics


def _transform_user_v1(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transform user record using v1 schema.
    
    DEPRECATED: V1 schema has issues with PII handling.
    TODO(TEAM-SEC): Remove after v2 migration
    """
    # TODO(TEAM-API): V1 transformation doesn't properly handle PII
    return {
        "user_id": record.get("id"),
        "registration_date": record.get("created_at"),
        "status": record.get("status"),
        "subscription_type": record.get("subscription_type"),
        "email_verified": bool(record.get("email_verified")),
        "days_since_last_login": _calculate_days_since(record.get("last_login_at")),
    }


def _transform_user_v2(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Transform user record using v2 schema."""
    return {
        "user_token": record.get("user_token"),
        "registration_date": record.get("created_at"),
        "status": record.get("status"),
        "subscription_tier": record.get("subscription_tier"),
        "email_verified": record.get("email_verified_at") is not None,
        "days_since_last_activity": _calculate_days_since(record.get("last_activity_at")),
        "country_code": record.get("country_code"),
        "signup_source": record.get("signup_source"),
    }


def _calculate_days_since(dt: Optional[datetime]) -> Optional[int]:
    """Calculate days since a given datetime."""
    if dt is None:
        return None
    delta = datetime.now() - dt
    return delta.days


def load_user_analytics(metrics: List[Dict[str, Any]], dry_run: bool = False) -> int:
    """
    Load transformed user metrics into analytics warehouse.
    
    Args:
        metrics: List of metric records to load.
        dry_run: If True, skip actual database writes.
    
    Returns:
        Number of records loaded.
    """
    if not metrics:
        logger.info("No metrics to load", extra={"record_count": 0})
        return 0
    
    logger.info(
        "Loading user analytics",
        extra={
            "record_count": len(metrics),
            "dry_run": dry_run,
        },
    )
    
    if dry_run:
        logger.info("Dry run - skipping database load")
        return len(metrics)
    
    # Load in batches
    loaded = 0
    for batch in batch_records(metrics):
        try:
            inserted = insert_analytics_batch("user_analytics", batch)
            loaded += inserted
        except Exception as e:
            logger.error(
                "Failed to load user analytics batch",
                extra={"error": str(e)},
            )
    
    logger.info(
        "User analytics load complete",
        extra={"loaded": loaded},
    )
    
    return loaded


def run_user_analytics_etl(
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run the complete user analytics ETL job.
    
    Args:
        start_date: Start of data extraction window.
        end_date: End of data extraction window.
        dry_run: If True, skip database writes.
    
    Returns:
        ETL result dictionary.
    """
    result = ETLResult(job_name="user_analytics")
    result.start_time = datetime.now()
    
    logger.info(
        "Starting user analytics ETL",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "dry_run": dry_run,
        },
    )
    
    try:
        # Extract
        raw_data = extract_user_data(start_date, end_date)
        result.records_extracted = len(raw_data)
        
        # Transform
        metrics = transform_user_metrics(
            raw_data,
            use_legacy_schema=is_v1_schema_enabled(),
        )
        result.records_transformed = len(metrics)
        
        # Load
        loaded = load_user_analytics(metrics, dry_run=dry_run)
        result.records_loaded = loaded
        result.records_processed = loaded
        
        result.status = "success"
        
    except Exception as e:
        result.status = "failed"
        result.errors.append(str(e))
        logger.error(
            "User analytics ETL failed",
            extra={"error": str(e)},
            exc_info=True,
        )
    
    result.end_time = datetime.now()
    result.duration_seconds = (result.end_time - result.start_time).total_seconds()
    
    logger.info(
        "User analytics ETL complete",
        extra=result.to_dict(),
    )
    
    return result.to_dict()
