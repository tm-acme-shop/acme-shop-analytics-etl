"""Regression tests for the v2-only user analytics schema path."""
from pathlib import Path
import runpy
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from acme_shop_analytics_etl.db import queries
from acme_shop_analytics_etl.etl import user_analytics_job as job


@pytest.mark.parametrize("v1_enabled", ["true", "false"])
def test_user_query_always_uses_v2(monkeypatch, date_range, v1_enabled):
    monkeypatch.setenv("ENABLE_V1_SCHEMA", v1_enabled)
    start_date, end_date = date_range
    records = [{"registration_date": start_date, "total_registrations": 3}]
    with patch.object(queries, "execute_parameterized_query", return_value=records) as execute:
        assert queries.fetch_user_analytics(start_date, end_date) == records

    query, params = execute.call_args.args
    assert "FROM users_v2" in query
    assert "%(start_date)s" in query
    assert "%(end_date)s" in query
    assert params == {"start_date": start_date, "end_date": end_date}


@pytest.mark.parametrize("v1_enabled", ["true", "false"])
def test_user_extraction_has_no_schema_selector(monkeypatch, date_range, v1_enabled):
    monkeypatch.setenv("ENABLE_V1_SCHEMA", v1_enabled)
    start_date, end_date = date_range
    with patch.object(job, "fetch_user_analytics", return_value=[]) as fetch:
        assert job.extract_user_data(start_date, end_date) == []
    fetch.assert_called_once_with(start_date=start_date, end_date=end_date)


@pytest.mark.parametrize("v1_enabled", ["true", "false"])
def test_user_transform_always_uses_v2(monkeypatch, sample_user_records, v1_enabled):
    monkeypatch.setenv("ENABLE_V1_SCHEMA", v1_enabled)
    # The separate legacy ETL flag must not select the v1 transformation either.
    monkeypatch.setenv("ENABLE_LEGACY_ETL", "true")
    metrics = job.transform_user_metrics(sample_user_records + sample_user_records)

    assert len(metrics) == len(sample_user_records)
    for record, metric in zip(sample_user_records, metrics):
        assert metric == {
            "user_token": record["user_token"],
            "registration_date": record["created_at"],
            "status": record["status"],
            "subscription_tier": record["subscription_tier"],
            "email_verified": record["email_verified_at"] is not None,
            "days_since_last_activity": job._calculate_days_since(record["last_activity_at"]),
            "country_code": record["country_code"],
            "signup_source": record["signup_source"],
        }
        assert "user_id" not in metric
        assert "subscription_type" not in metric


def test_user_transform_handles_empty_data():
    assert job.transform_user_metrics([]) == []


@pytest.mark.parametrize("v1_enabled", ["true", "false"])
def test_user_etl_uses_v2_transform(monkeypatch, date_range, sample_user_records, v1_enabled):
    monkeypatch.setenv("ENABLE_V1_SCHEMA", v1_enabled)
    with patch.object(job, "extract_user_data", return_value=sample_user_records):
        result = job.run_user_analytics_etl(*date_range, dry_run=True)
    assert result["status"] == "success"
    assert result["records_transformed"] == len(sample_user_records)
    assert result["records_loaded"] == len(sample_user_records)


@pytest.mark.parametrize("v1_enabled", ["true", "false"])
def test_user_dag_transform_has_no_schema_selector(monkeypatch, sample_user_records, v1_enabled):
    monkeypatch.setenv("ENABLE_V1_SCHEMA", v1_enabled)
    # Stub only Airflow's DAG construction; exercise the real task callable.
    airflow = ModuleType("airflow")
    airflow.DAG = MagicMock()
    operators = ModuleType("airflow.operators")
    python_operator = ModuleType("airflow.operators.python")
    python_operator.PythonOperator = MagicMock()
    dag_path = Path(__file__).resolve().parents[1] / "dags" / "user_analytics_dag.py"
    with patch.dict("sys.modules", {
        "airflow": airflow,
        "airflow.operators": operators,
        "airflow.operators.python": python_operator,
    }), patch.object(job, "transform_user_metrics", return_value=[]) as transform:
        dag = runpy.run_path(str(dag_path))
        ti = MagicMock()
        ti.xcom_pull.return_value = sample_user_records
        assert dag["run_transform"](ti=ti) == []

    ti.xcom_pull.assert_called_once_with(task_ids="extract_users")
    transform.assert_called_once_with(sample_user_records)
