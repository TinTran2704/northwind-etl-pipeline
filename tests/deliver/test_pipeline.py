"""Tests for deliver phase pipeline orchestrator."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.deliver.pipeline import DeliverResult, run_deliver_phase


def _mock_engine():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value = None
    begin_conn = engine.begin.return_value.__enter__.return_value
    begin_conn.execute.return_value = None
    return engine


class TestDeliverResult:
    def test_defaults(self):
        r = DeliverResult(batch_id="b1")
        assert r.dims_loaded == {}
        assert r.fact_rows == 0
        assert r.agg_rows == 0
        assert r.errors == []


class TestRunDeliverPhase:
    @patch("src.deliver.pipeline._mark_etl_run")
    @patch("src.deliver.pipeline._load_dims_from_db")
    @patch("src.deliver.pipeline._read_csv")
    @patch("src.deliver.pipeline.DimBuilder")
    @patch("src.deliver.pipeline.FactBuilder")
    @patch("src.deliver.pipeline.AggregateBuilder")
    def test_returns_deliver_result(
        self, MockAgg, MockFact, MockDim, mock_read_csv,
        mock_load_db, mock_mark, tmp_path,
    ):
        # DimBuilder.load_all_dims → {"dim_customer": 10}
        mock_dim_instance = MockDim.return_value
        mock_dim_instance.load_all_dims.return_value = {"dim_customer": 10}

        # _load_dims_from_db → minimal dims
        mock_load_db.return_value = {}

        # _read_csv: orders empty so fact step is skipped
        mock_read_csv.return_value = pd.DataFrame()

        # AggregateBuilder
        mock_agg_instance = MockAgg.return_value
        mock_agg_instance.build_agg_sales_monthly.return_value = pd.DataFrame()
        mock_agg_instance.load_agg_to_postgres.return_value = 0

        engine = _mock_engine()
        result = run_deliver_phase(
            "batch-001", tmp_path, engine,
            conformed_dir=tmp_path, raw_dir=tmp_path,
        )

        assert isinstance(result, DeliverResult)
        assert result.batch_id == "batch-001"
        assert result.dims_loaded == {"dim_customer": 10}

    @patch("src.deliver.pipeline._mark_etl_run")
    @patch("src.deliver.pipeline._load_dims_from_db")
    @patch("src.deliver.pipeline._read_csv")
    @patch("src.deliver.pipeline.DimBuilder")
    @patch("src.deliver.pipeline.FactBuilder")
    @patch("src.deliver.pipeline.AggregateBuilder")
    def test_dim_load_failure_returns_early(
        self, MockAgg, MockFact, MockDim, mock_read_csv,
        mock_load_db, mock_mark, tmp_path,
    ):
        mock_dim_instance = MockDim.return_value
        mock_dim_instance.load_all_dims.side_effect = RuntimeError("DB down")

        engine = _mock_engine()
        result = run_deliver_phase(
            "batch-002", tmp_path, engine,
            conformed_dir=tmp_path, raw_dir=tmp_path,
        )

        assert len(result.errors) == 1
        assert "load_all_dims failed" in result.errors[0]
        assert result.dims_loaded == {}

    @patch("src.deliver.pipeline._mark_etl_run")
    @patch("src.deliver.pipeline._load_dims_from_db")
    @patch("src.deliver.pipeline._read_csv")
    @patch("src.deliver.pipeline.DimBuilder")
    @patch("src.deliver.pipeline.FactBuilder")
    @patch("src.deliver.pipeline.AggregateBuilder")
    def test_mark_etl_run_called_on_success(
        self, MockAgg, MockFact, MockDim, mock_read_csv,
        mock_load_db, mock_mark, tmp_path,
    ):
        mock_dim_instance = MockDim.return_value
        mock_dim_instance.load_all_dims.return_value = {"dim_date": 100}
        mock_load_db.return_value = {}
        mock_read_csv.return_value = pd.DataFrame()

        mock_agg_instance = MockAgg.return_value
        mock_agg_instance.build_agg_sales_monthly.return_value = pd.DataFrame()
        mock_agg_instance.load_agg_to_postgres.return_value = 0

        engine = _mock_engine()
        run_deliver_phase(
            "batch-003", tmp_path, engine,
            conformed_dir=tmp_path, raw_dir=tmp_path,
        )

        mock_mark.assert_called_once()
        call_kwargs = mock_mark.call_args
        assert call_kwargs[0][0] == "batch-003"

    @patch("src.deliver.pipeline._mark_etl_run")
    @patch("src.deliver.pipeline._load_dims_from_db")
    @patch("src.deliver.pipeline._read_csv")
    @patch("src.deliver.pipeline.DimBuilder")
    @patch("src.deliver.pipeline.FactBuilder")
    @patch("src.deliver.pipeline.AggregateBuilder")
    def test_fact_build_error_appended_to_errors(
        self, MockAgg, MockFact, MockDim, mock_read_csv,
        mock_load_db, mock_mark, tmp_path,
    ):
        mock_dim_instance = MockDim.return_value
        mock_dim_instance.load_all_dims.return_value = {"dim_date": 100}
        mock_load_db.return_value = {}

        # Return non-empty orders so fact step is attempted
        orders_df = pd.DataFrame([{"orderID": 1}])
        mock_read_csv.return_value = orders_df

        mock_fact_instance = MockFact.return_value
        mock_fact_instance.build_fact_sales.side_effect = RuntimeError("Fact error")

        mock_agg_instance = MockAgg.return_value
        mock_agg_instance.build_agg_sales_monthly.return_value = pd.DataFrame()
        mock_agg_instance.load_agg_to_postgres.return_value = 0

        engine = _mock_engine()
        result = run_deliver_phase(
            "batch-004", tmp_path, engine,
            conformed_dir=tmp_path, raw_dir=tmp_path,
        )

        fact_errors = [e for e in result.errors if "fact_sales" in e]
        assert len(fact_errors) == 1
