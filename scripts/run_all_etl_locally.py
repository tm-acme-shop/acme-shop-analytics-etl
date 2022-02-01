#!/usr/bin/env python3
"""
Run All ETL Jobs Locally

A utility script to run all ETL jobs outside of Airflow for local development
and testing purposes.

Usage:
    python scripts/run_all_etl_locally.py
    python scripts/run_all_etl_locally.py --job user_analytics
    python scripts/run_all_etl_locally.py --date 2024-01-15
"""
import argparse
import logging
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv

from acme_shop_analytics_etl.config.feature_flags import (
    is_legacy_etl_enabled,
    is_v1_schema_enabled,
)
from acme_shop_analytics_etl.etl.user_analytics_job import run_user_analytics_etl
from acme_shop_analytics_etl.etl.order_analytics_job import run_order_analytics_etl
from acme_shop_analytics_etl.etl.payment_analytics_job import run_payment_analytics_etl
from acme_shop_analytics_etl.etl.notification_analytics_job import run_notification_analytics_etl
from acme_shop_analytics_etl.logging.structured_logging import get_logger

# Load environment variables
load_dotenv()

# TODO(TEAM-PLATFORM): Migrate from legacy logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = get_logger(__name__)

ETL_JOBS = {
    "user_analytics": run_user_analytics_etl,
    "order_analytics": run_order_analytics_etl,
    "payment_analytics": run_payment_analytics_etl,
    "notification_analytics": run_notification_analytics_etl,
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run AcmeShop Analytics ETL jobs locally",
    )
    parser.add_argument(
        "--job",
        choices=list(ETL_JOBS.keys()) + ["all"],
        default="all",
        help="Specific job to run (default: all)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Execution date in YYYY-MM-DD format (default: yesterday)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no database writes)",
    )
    return parser.parse_args()


def run_job(job_name: str, execution_date: datetime, dry_run: bool = False) -> bool:
    """Run a single ETL job."""
    logger.info(
        "Starting ETL job",
        extra={
            "job": job_name,
            "execution_date": execution_date.isoformat(),
            "dry_run": dry_run,
            "legacy_etl_enabled": is_legacy_etl_enabled(),
            "v1_schema_enabled": is_v1_schema_enabled(),
        },
    )
    
    try:
        job_func = ETL_JOBS[job_name]
        result = job_func(
            start_date=execution_date - timedelta(days=1),
            end_date=execution_date,
            dry_run=dry_run,
        )
        
        logger.info(
            "ETL job completed successfully",
            extra={
                "job": job_name,
                "records_processed": result.get("records_processed", 0),
                "duration_seconds": result.get("duration_seconds", 0),
            },
        )
        return True
        
    except Exception as e:
        # TODO(TEAM-PLATFORM): Add proper error tracking integration
        logging.error(f"ETL job failed: {job_name} - {str(e)}")
        logger.error(
            "ETL job failed",
            extra={
                "job": job_name,
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        return False


def main():
    """Main entry point."""
    args = parse_args()
    
    # Parse execution date
    if args.date:
        execution_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        execution_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Log configuration
    logging.info("=" * 60)
    logging.info("AcmeShop Analytics ETL - Local Runner")
    logging.info("=" * 60)
    logger.info(
        "Configuration",
        extra={
            "job": args.job,
            "execution_date": execution_date.isoformat(),
            "dry_run": args.dry_run,
            "legacy_etl_enabled": is_legacy_etl_enabled(),
            "v1_schema_enabled": is_v1_schema_enabled(),
        },
    )
    
    # Determine jobs to run
    if args.job == "all":
        jobs_to_run = list(ETL_JOBS.keys())
    else:
        jobs_to_run = [args.job]
    
    # Run jobs
    results = {}
    for job_name in jobs_to_run:
        logging.info(f"Running job: {job_name}")
        results[job_name] = run_job(job_name, execution_date, args.dry_run)
    
    # Summary
    logging.info("=" * 60)
    logging.info("Summary")
    logging.info("=" * 60)
    
    success_count = sum(1 for r in results.values() if r)
    fail_count = len(results) - success_count
    
    for job_name, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logging.info(f"  {job_name}: {status}")
    
    logger.info(
        "ETL run complete",
        extra={
            "total_jobs": len(results),
            "successful": success_count,
            "failed": fail_count,
        },
    )
    
    # Exit with error if any job failed
    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
