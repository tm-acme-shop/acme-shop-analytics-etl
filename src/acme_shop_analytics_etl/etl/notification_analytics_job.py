"""
Notification Analytics ETL Job

Processes notification data for analytics including delivery rates,
engagement metrics, and channel performance.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

from acme_shop_analytics_etl.config.feature_flags import is_v1_schema_enabled
from acme_shop_analytics_etl.db.queries import fetch_notification_analytics, insert_analytics_batch
from acme_shop_analytics_etl.etl.common import ETLResult, batch_records
from acme_shop_analytics_etl.etl.deduplication import RecordDeduplicator
from acme_shop_analytics_etl.logging.structured_logging import get_logger, log_context

logger = get_logger(__name__)


def extract_notification_data(
    start_date: datetime,
    end_date: datetime,
    channels: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Extract notification data from source database.
    
    Args:
        start_date: Start of extraction window.
        end_date: End of extraction window.
        channels: Optional list of channels to filter.
    
    Returns:
        List of notification records.
    """
    channels = channels or ["email", "sms", "push"]
    
    logger.info(
        "Extracting notification data",
        extra={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "channels": channels,
        },
    )
    
    return fetch_notification_analytics(
        start_date=start_date,
        end_date=end_date,
        channels=channels,
    )


def transform_notification_metrics(
    raw_data: List[Dict[str, Any]],
    use_legacy_schema: bool = False,
) -> List[Dict[str, Any]]:
    """
    Transform raw notification data into analytics metrics.
    
    Args:
        raw_data: List of raw notification records.
        use_legacy_schema: If True, use v1 transformations.
    
    Returns:
        List of transformed metric records.
    """
    if not raw_data:
        logger.info("No notification data to transform", extra={"record_count": 0})
        return []
    
    logger.info(
        "Transforming notification metrics",
        extra={
            "record_count": len(raw_data),
            "schema": "v1" if use_legacy_schema else "v2",
        },
    )
    
    # Deduplicate
    deduplicator = RecordDeduplicator(use_legacy_hash=use_legacy_schema)
    unique_records = deduplicator.deduplicate_batch(raw_data)
    
    metrics = []
    for record in unique_records:
        try:
            total_sent = record.get("total_sent", 0)
            delivered = record.get("delivered", 0)
            opened = record.get("opened", 0)
            clicked = record.get("clicked", 0)
            
            metric = {
                "notification_date": record.get("notification_date"),
                "channel": record.get("channel"),
                "notification_type": record.get("notification_type"),
                "total_sent": total_sent,
                "delivered": delivered,
                "opened": opened,
                "clicked": clicked,
                "delivery_rate": _calculate_rate(delivered, total_sent),
                "open_rate": _calculate_rate(opened, delivered),
                "click_rate": _calculate_rate(clicked, opened),
                "click_through_rate": _calculate_rate(clicked, total_sent),
            }
            
            # V2 schema includes additional metrics
            if not use_legacy_schema:
                metric["bounced"] = record.get("bounced", 0)
                metric["failed"] = record.get("failed", 0)
            
            metrics.append(metric)
            
        except Exception as e:
            logger.warning(
                "Failed to transform notification record",
                extra={"error": str(e)},
            )
    
    logger.info(
        "Notification transformation complete",
        extra={
            "input_count": len(raw_data),
            "output_count": len(metrics),
        },
    )
    
    return metrics


def _calculate_rate(numerator: int, denominator: int) -> float:
    """Calculate a rate percentage."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100, 2)


def calculate_channel_metrics(
    transformed_data: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Calculate aggregate metrics per channel.
    
    Args:
        transformed_data: List of transformed notification records.
    
    Returns:
        Dictionary of channel metrics.
    """
    if not transformed_data:
        return {}
    
    channels: Dict[str, Dict[str, Any]] = {}
    
    for record in transformed_data:
        channel = record.get("channel")
        if not channel:
            continue
        
        if channel not in channels:
            channels[channel] = {
                "total_sent": 0,
                "delivered": 0,
                "opened": 0,
                "clicked": 0,
            }
        
        channels[channel]["total_sent"] += record.get("total_sent", 0)
        channels[channel]["delivered"] += record.get("delivered", 0)
        channels[channel]["opened"] += record.get("opened", 0)
        channels[channel]["clicked"] += record.get("clicked", 0)
    
    # Calculate rates for each channel
    for channel, data in channels.items():
        data["delivery_rate"] = _calculate_rate(data["delivered"], data["total_sent"])
        data["open_rate"] = _calculate_rate(data["opened"], data["delivered"])
        data["click_rate"] = _calculate_rate(data["clicked"], data["opened"])
        
        logger.info(
            "Channel metrics calculated",
            extra={
                "channel": channel,
                "delivery_rate": data["delivery_rate"],
                "open_rate": data["open_rate"],
                "click_rate": data["click_rate"],
            },
        )
    
    return channels


def load_notification_analytics(
    notification_metrics: List[Dict[str, Any]],
    channel_metrics: Optional[Dict[str, Dict[str, Any]]] = None,
    dry_run: bool = False,
) -> int:
    """
    Load transformed notification metrics into analytics warehouse.
    
    Args:
        notification_metrics: List of notification metric records.
        channel_metrics: Optional aggregated channel metrics.
        dry_run: If True, skip actual database writes.
    
    Returns:
        Number of records loaded.
    """
    if not notification_metrics:
        logger.info("No notification metrics to load", extra={"record_count": 0})
        return 0
    
    logger.info(
        "Loading notification analytics",
        extra={
            "record_count": len(notification_metrics),
            "channel_count": len(channel_metrics) if channel_metrics else 0,
            "dry_run": dry_run,
        },
    )
    
    if dry_run:
        logger.info("Dry run - skipping database load")
        return len(notification_metrics)
    
    loaded = 0
    
    # Load notification metrics
    for batch in batch_records(notification_metrics):
        try:
            inserted = insert_analytics_batch("notification_analytics", batch)
            loaded += inserted
        except Exception as e:
            logger.error(
                "Failed to load notification analytics batch",
                extra={"error": str(e)},
            )
    
    # Load channel metrics if provided
    if channel_metrics:
        channel_records = [
            {"channel": channel, **data}
            for channel, data in channel_metrics.items()
        ]
        try:
            insert_analytics_batch("channel_analytics", channel_records)
        except Exception as e:
            logger.error(
                "Failed to load channel analytics",
                extra={"error": str(e)},
            )
    
    logger.info(
        "Notification analytics load complete",
        extra={"loaded": loaded},
    )
    
    return loaded


def run_notification_analytics_etl(
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run the complete notification analytics ETL job.
    
    Args:
        start_date: Start of data extraction window.
        end_date: End of data extraction window.
        dry_run: If True, skip database writes.
    
    Returns:
        ETL result dictionary.
    """
    result = ETLResult(job_name="notification_analytics")
    result.start_time = datetime.now()
    
    # Use structured logging context for correlation
    with log_context(
        job="notification_analytics",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    ):
        logger.info(
            "Starting notification analytics ETL",
            extra={"dry_run": dry_run},
        )
        
        try:
            # Extract
            raw_data = extract_notification_data(start_date, end_date)
            result.records_extracted = len(raw_data)
            
            # Transform
            metrics = transform_notification_metrics(
                raw_data,
                use_legacy_schema=is_v1_schema_enabled(),
            )
            result.records_transformed = len(metrics)
            
            # Calculate channel metrics
            channel_metrics = calculate_channel_metrics(metrics)
            
            # Load
            loaded = load_notification_analytics(
                notification_metrics=metrics,
                channel_metrics=channel_metrics,
                dry_run=dry_run,
            )
            result.records_loaded = loaded
            result.records_processed = loaded
            
            result.status = "success"
            
        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            logger.error(
                "Notification analytics ETL failed",
                extra={"error": str(e)},
                exc_info=True,
            )
        
        result.end_time = datetime.now()
        result.duration_seconds = (result.end_time - result.start_time).total_seconds()
        
        logger.info(
            "Notification analytics ETL complete",
            extra=result.to_dict(),
        )
    
    return result.to_dict()
