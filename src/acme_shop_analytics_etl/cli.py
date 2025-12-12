"""
AcmeShop Analytics ETL CLI

Command-line interface for running ETL jobs manually.
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional

from acme_shop_analytics_etl.config.settings import get_settings
from acme_shop_analytics_etl.config.feature_flags import (
    get_feature_flags,
    is_legacy_etl_enabled,
    is_legacy_pii_enabled,
)
from acme_shop_analytics_etl.etl import (
    run_user_analytics_etl,
    run_order_analytics_etl,
    run_payment_analytics_etl,
    run_notification_analytics_etl,
)
from acme_shop_analytics_etl.logging.structured_logging import (
    configure_logging,
    get_logger,
)

logger = get_logger(__name__)


AVAILABLE_JOBS = {
    "user": run_user_analytics_etl,
    "order": run_order_analytics_etl,
    "payment": run_payment_analytics_etl,
    "notification": run_notification_analytics_etl,
}


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string in YYYY-MM-DD format.
    
    Args:
        date_str: Date string to parse.
    
    Returns:
        Parsed datetime object.
    
    Raises:
        argparse.ArgumentTypeError: If date format is invalid.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Expected YYYY-MM-DD"
        )


def run_job(
    job_name: str,
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
) -> dict:
    """
    Run a specific ETL job.
    
    Args:
        job_name: Name of the job to run.
        start_date: Start of data extraction window.
        end_date: End of data extraction window.
        dry_run: If True, skip database writes.
    
    Returns:
        ETL result dictionary.
    
    Raises:
        ValueError: If job name is invalid.
    """
    if job_name not in AVAILABLE_JOBS:
        raise ValueError(f"Unknown job: {job_name}. Available: {list(AVAILABLE_JOBS.keys())}")
    
    job_func = AVAILABLE_JOBS[job_name]
    return job_func(start_date=start_date, end_date=end_date, dry_run=dry_run)


def run_all_jobs(
    start_date: datetime,
    end_date: datetime,
    dry_run: bool = False,
) -> list:
    """
    Run all ETL jobs in sequence.
    
    TODO(TEAM-PLATFORM): Consider parallel execution for independent jobs.
    
    Args:
        start_date: Start of data extraction window.
        end_date: End of data extraction window.
        dry_run: If True, skip database writes.
    
    Returns:
        List of ETL result dictionaries.
    """
    results = []
    
    for job_name in AVAILABLE_JOBS:
        logger.info(f"Running job: {job_name}")
        try:
            result = run_job(job_name, start_date, end_date, dry_run)
            results.append(result)
        except Exception as e:
            logger.error(f"Job {job_name} failed: {e}")
            results.append({"job_name": job_name, "status": "failed", "error": str(e)})
    
    return results


def print_status() -> None:
    """Print current configuration and feature flag status."""
    settings = get_settings()
    flags = get_feature_flags()
    
    print("\n=== AcmeShop Analytics ETL ===\n")
    print("Configuration:")
    print(f"  Database: {settings.database.url[:50]}...")
    print(f"  ETL Batch Size: {settings.etl.batch_size}")
    print(f"  Max Retries: {settings.etl.max_retries}")
    print()
    print("Feature Flags:")
    print(f"  ENABLE_LEGACY_ETL: {flags.enable_legacy_etl}")
    print(f"  ENABLE_V1_SCHEMA: {flags.enable_v1_schema}")
    print(f"  ENABLE_LEGACY_PAYMENTS: {flags.enable_legacy_payments}")
    print(f"  ENABLE_LEGACY_PII: {flags.enable_legacy_pii}")
    print(f"  ENABLE_EXPERIMENTAL_DEDUP: {flags.enable_experimental_dedup}")
    print()
    
    # TODO(TEAM-PLATFORM): Add warnings for deprecated flags
    if is_legacy_etl_enabled():
        print("⚠️  WARNING: Legacy ETL is enabled. Consider migrating to v2.")
    if is_legacy_pii_enabled():
        print("⚠️  WARNING: Legacy PII handling is enabled. Security risk!")
    print()


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="acme-etl",
        description="AcmeShop Analytics ETL CLI",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Run command
    run_parser = subparsers.add_parser("run", help="Run ETL job(s)")
    run_parser.add_argument(
        "job",
        nargs="?",
        default="all",
        choices=["all"] + list(AVAILABLE_JOBS.keys()),
        help="Job to run (default: all)",
    )
    run_parser.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date (YYYY-MM-DD). Default: yesterday",
    )
    run_parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date (YYYY-MM-DD). Default: today",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip database writes",
    )
    run_parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Number of days back to process (default: 1)",
    )
    
    # Status command
    subparsers.add_parser("status", help="Show configuration and feature flags")
    
    # Backfill command
    backfill_parser = subparsers.add_parser("backfill", help="Backfill historical data")
    backfill_parser.add_argument(
        "job",
        choices=list(AVAILABLE_JOBS.keys()),
        help="Job to backfill",
    )
    backfill_parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    backfill_parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    backfill_parser.add_argument(
        "--batch-days",
        type=int,
        default=7,
        help="Days per batch (default: 7)",
    )
    backfill_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip database writes",
    )
    
    # Common options
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs in JSON format",
    )
    
    return parser


def run_backfill(
    job_name: str,
    start_date: datetime,
    end_date: datetime,
    batch_days: int = 7,
    dry_run: bool = False,
) -> list:
    """
    Run backfill for a job over a date range.
    
    TODO(TEAM-PLATFORM): Add checkpointing for resumable backfills.
    
    Args:
        job_name: Job to backfill.
        start_date: Start of backfill range.
        end_date: End of backfill range.
        batch_days: Days per batch.
        dry_run: Skip database writes.
    
    Returns:
        List of batch results.
    """
    results = []
    current = start_date
    batch_delta = timedelta(days=batch_days)
    
    logger.info(
        "Starting backfill",
        extra={
            "job": job_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "batch_days": batch_days,
        },
    )
    
    while current < end_date:
        batch_end = min(current + batch_delta, end_date)
        
        logger.info(
            f"Processing batch: {current.date()} to {batch_end.date()}",
            extra={
                "batch_start": current.isoformat(),
                "batch_end": batch_end.isoformat(),
            },
        )
        
        try:
            result = run_job(job_name, current, batch_end, dry_run)
            results.append(result)
        except Exception as e:
            logger.error(f"Batch failed: {e}")
            results.append({
                "job_name": job_name,
                "status": "failed",
                "batch_start": current.isoformat(),
                "batch_end": batch_end.isoformat(),
                "error": str(e),
            })
        
        current = batch_end
    
    # Summary
    successful = sum(1 for r in results if r.get("status") == "success")
    failed = len(results) - successful
    
    logger.info(
        "Backfill complete",
        extra={
            "total_batches": len(results),
            "successful": successful,
            "failed": failed,
        },
    )
    
    return results


def main(argv: Optional[list] = None) -> int:
    """
    Main entry point for the CLI.
    
    Args:
        argv: Command line arguments (defaults to sys.argv).
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # Configure logging
    log_level = "DEBUG" if args.verbose else "INFO"
    configure_logging(level=log_level, use_json=args.json_logs)
    
    if args.command == "status":
        print_status()
        return 0
    
    elif args.command == "run":
        # Determine date range
        if args.end_date:
            end_date = args.end_date
        else:
            end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        if args.start_date:
            start_date = args.start_date
        else:
            start_date = end_date - timedelta(days=args.days_back)
        
        logger.info(
            f"Running ETL: {args.job}",
            extra={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "dry_run": args.dry_run,
            },
        )
        
        try:
            if args.job == "all":
                results = run_all_jobs(start_date, end_date, args.dry_run)
            else:
                results = [run_job(args.job, start_date, end_date, args.dry_run)]
            
            # Check for failures
            failed = [r for r in results if r.get("status") != "success"]
            if failed:
                logger.error(f"{len(failed)} job(s) failed")
                return 1
            
            logger.info("All jobs completed successfully")
            return 0
            
        except Exception as e:
            logger.error(f"ETL failed: {e}", exc_info=True)
            return 1
    
    elif args.command == "backfill":
        try:
            results = run_backfill(
                job_name=args.job,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_days=args.batch_days,
                dry_run=args.dry_run,
            )
            
            failed = [r for r in results if r.get("status") != "success"]
            if failed:
                logger.error(f"{len(failed)} batch(es) failed")
                return 1
            
            return 0
            
        except Exception as e:
            logger.error(f"Backfill failed: {e}", exc_info=True)
            return 1
    
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
