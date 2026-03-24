"""
Pytest suite for flink_job/Payment_stream_Processor_flink.py

Tested units
------------
- create_table_if_not_exists   DB setup helper
- ParseEvent.process_element   JSON parsing + DLQ routing
- PaymentAggregator.process    window aggregation logic

All pyflink / psycopg2 imports are stubbed in conftest.py so the module
can be imported without a running Flink cluster or database.
"""

import json
import re
import sys
import pytest
from unittest.mock import MagicMock

# conftest.py has already injected pyflink and psycopg2 stubs into sys.modules,
# so importing the module below will execute its top-level setup code safely.
import Payment_stream_Processor_flink as proc

ParseEvent = proc.ParseEvent
PaymentAggregator = proc.PaymentAggregator


# ---------------------------------------------------------------------------
# Helper stubs
# ---------------------------------------------------------------------------

class _FakeCtx:
    """Minimal ProcessFunction context that records side-output calls."""

    def __init__(self):
        self.side_outputs: dict[str, list] = {}

    def output(self, tag, value):
        self.side_outputs.setdefault(tag.tag_id, []).append(value)


class _FakeWindow:
    def __init__(self, start_ms: int = 0):
        self.start = start_ms


class _FakeWindowCtx:
    """Minimal ProcessWindowFunction context."""

    def __init__(self, window_start_ms: int = 0):
        self._window = _FakeWindow(window_start_ms)

    def window(self):
        return self._window


def _elem(bank_code: str, status: str, amount: float):
    """Shorthand to create a parsed-stream element tuple."""
    return (bank_code, status, float(amount))


def _run_aggregator(key, elements, window_start_ms=0):
    agg = PaymentAggregator()
    ctx = _FakeWindowCtx(window_start_ms)
    return list(agg.process(key, ctx, elements))


# ===========================================================================
# create_table_if_not_exists
# ===========================================================================

class TestCreateTableIfNotExists:

    def test_connects_using_default_env_values(self):
        """Should connect to Postgres with the default env var values."""
        mock_psycopg2 = sys.modules["psycopg2"]
        proc.create_table_if_not_exists()

        mock_psycopg2.connect.assert_called_with(
            host="postgres",
            port="5432",
            dbname="payments",
            user="postgres",
            password="postgres",
        )

    def test_executes_create_table_sql(self):
        """Should call cursor.execute() with a CREATE TABLE IF NOT EXISTS statement."""
        mock_psycopg2 = sys.modules["psycopg2"]

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = lambda _: mock_conn
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = lambda _: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn

        proc.create_table_if_not_exists()

        sql = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "payment_aggregates" in sql

    def test_respects_env_variable_overrides(self, monkeypatch):
        """Should use custom env vars when they are set."""
        monkeypatch.setenv("POSTGRES_HOST", "myhost")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "myuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "mypass")

        mock_psycopg2 = sys.modules["psycopg2"]
        proc.create_table_if_not_exists()

        mock_psycopg2.connect.assert_called_with(
            host="myhost",
            port="5433",
            dbname="mydb",
            user="myuser",
            password="mypass",
        )


# ===========================================================================
# ParseEvent.process_element
# ===========================================================================

class TestParseEvent:

    def setup_method(self):
        self.parser = ParseEvent()
        self.ctx = _FakeCtx()

    def _parse(self, msg: str):
        return list(self.parser.process_element(msg, self.ctx))

    # --- happy path ---

    def test_valid_event_yields_one_tuple(self):
        msg = json.dumps({"bank_code": "ANZ", "status": "SUCCESS", "amount": "150.50"})
        results = self._parse(msg)
        assert len(results) == 1

    def test_valid_event_fields_are_correct(self):
        msg = json.dumps({"bank_code": "ANZ", "status": "SUCCESS", "amount": "150.50"})
        bank_code, status, amount = self._parse(msg)[0]
        assert bank_code == "ANZ"
        assert status == "SUCCESS"
        assert amount == pytest.approx(150.50)

    def test_amount_is_cast_to_float(self):
        msg = json.dumps({"bank_code": "CBA", "status": "SUCCESS", "amount": "200"})
        _, _, amount = self._parse(msg)[0]
        assert isinstance(amount, float)

    def test_missing_amount_defaults_to_zero(self):
        msg = json.dumps({"bank_code": "NAB", "status": "FAILED"})
        _, _, amount = self._parse(msg)[0]
        assert amount == 0.0

    def test_valid_event_does_not_produce_dlq_output(self):
        msg = json.dumps({"bank_code": "WBC", "status": "SUCCESS", "amount": "99"})
        self._parse(msg)
        assert "dlq" not in self.ctx.side_outputs

    def test_failed_status_is_passed_through(self):
        msg = json.dumps({"bank_code": "SBI", "status": "FAILED", "amount": "50"})
        _, status, _ = self._parse(msg)[0]
        assert status == "FAILED"

    # --- DLQ routing ---

    def test_invalid_json_yields_nothing(self):
        results = self._parse("not-valid-json{{")
        assert results == []

    def test_invalid_json_routes_to_dlq(self):
        self._parse("not-valid-json{{")
        assert "dlq" in self.ctx.side_outputs

    def test_dlq_entry_contains_raw_message(self):
        bad = "not-valid-json{{"
        self._parse(bad)
        entry = json.loads(self.ctx.side_outputs["dlq"][0])
        assert entry["raw"] == bad

    def test_dlq_entry_contains_error_description(self):
        self._parse("{bad json}")
        entry = json.loads(self.ctx.side_outputs["dlq"][0])
        assert "error" in entry and entry["error"]

    def test_missing_bank_code_routes_to_dlq(self):
        msg = json.dumps({"status": "SUCCESS", "amount": "50"})
        results = self._parse(msg)
        assert results == []
        assert "dlq" in self.ctx.side_outputs

    def test_missing_status_routes_to_dlq(self):
        msg = json.dumps({"bank_code": "ANZ", "amount": "75"})
        results = self._parse(msg)
        assert results == []
        assert "dlq" in self.ctx.side_outputs

    def test_empty_string_routes_to_dlq(self):
        results = self._parse("")
        assert results == []
        assert "dlq" in self.ctx.side_outputs

    def test_multiple_valid_messages_each_yield_one_result(self):
        """process_element is called once per message — each call yields one tuple."""
        for bank in ["ANZ", "CBA", "NAB"]:
            ctx = _FakeCtx()
            msg = json.dumps({"bank_code": bank, "status": "SUCCESS", "amount": "10"})
            results = list(self.parser.process_element(msg, ctx))
            assert len(results) == 1


# ===========================================================================
# PaymentAggregator.process
# ===========================================================================

class TestPaymentAggregator:

    # --- success rate ---

    def test_all_success_gives_rate_of_one(self):
        rows = _run_aggregator("ANZ", [
            _elem("ANZ", "SUCCESS", 100),
            _elem("ANZ", "SUCCESS", 200),
        ])
        assert rows[0].f3 == pytest.approx(1.0)

    def test_all_failed_gives_rate_of_zero(self):
        rows = _run_aggregator("CBA", [
            _elem("CBA", "FAILED", 50),
            _elem("CBA", "FAILED", 75),
        ])
        assert rows[0].f3 == pytest.approx(0.0)

    def test_mixed_transactions_correct_success_rate(self):
        # 3 success + 1 failed  →  0.75
        rows = _run_aggregator("NAB", [
            _elem("NAB", "SUCCESS", 10),
            _elem("NAB", "SUCCESS", 20),
            _elem("NAB", "SUCCESS", 30),
            _elem("NAB", "FAILED",  40),
        ])
        assert rows[0].f3 == pytest.approx(0.75)

    def test_single_success_transaction(self):
        rows = _run_aggregator("ANZ", [_elem("ANZ", "SUCCESS", 100)])
        assert rows[0].f3 == pytest.approx(1.0)

    def test_single_failed_transaction(self):
        rows = _run_aggregator("ANZ", [_elem("ANZ", "FAILED", 100)])
        assert rows[0].f3 == pytest.approx(0.0)

    # --- counters ---

    def test_total_transactions_count(self):
        rows = _run_aggregator("ANZ", [
            _elem("ANZ", "SUCCESS", 1),
            _elem("ANZ", "FAILED",  2),
            _elem("ANZ", "SUCCESS", 3),
        ])
        assert rows[0].f1 == 3

    def test_failed_transactions_count(self):
        rows = _run_aggregator("ANZ", [
            _elem("ANZ", "SUCCESS", 1),
            _elem("ANZ", "FAILED",  2),
            _elem("ANZ", "FAILED",  3),
        ])
        assert rows[0].f2 == 2

    def test_failed_count_is_zero_when_all_success(self):
        rows = _run_aggregator("WBC", [_elem("WBC", "SUCCESS", 50)])
        assert rows[0].f2 == 0

    # --- total amount ---

    def test_total_amount_is_summed(self):
        rows = _run_aggregator("ANZ", [
            _elem("ANZ", "SUCCESS", 100.0),
            _elem("ANZ", "SUCCESS", 200.0),
        ])
        assert rows[0].f4 == pytest.approx(300.0)

    def test_total_amount_rounded_to_two_decimal_places(self):
        rows = _run_aggregator("ANZ", [
            _elem("ANZ", "SUCCESS", 10.005),
            _elem("ANZ", "SUCCESS", 20.005),
        ])
        assert rows[0].f4 == round(10.005 + 20.005, 2)

    # --- window_start ---

    def test_window_start_has_correct_datetime_format(self):
        pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
        rows = _run_aggregator("ANZ", [_elem("ANZ", "SUCCESS", 1)], window_start_ms=0)
        assert re.match(pattern, rows[0].f5), f"Bad format: {rows[0].f5}"

    def test_window_start_derived_from_context(self):
        # 2024-01-15 00:00:00 UTC = 1705276800 seconds
        rows_a = _run_aggregator("ANZ", [_elem("ANZ", "SUCCESS", 1)], window_start_ms=0)
        rows_b = _run_aggregator("ANZ", [_elem("ANZ", "SUCCESS", 1)], window_start_ms=1705276800_000)
        assert rows_a[0].f5 != rows_b[0].f5

    # --- output row structure ---

    def test_output_yields_exactly_one_row(self):
        rows = _run_aggregator("ANZ", [_elem("ANZ", "SUCCESS", 100)])
        assert len(rows) == 1

    def test_bank_code_matches_key(self):
        rows = _run_aggregator("MY_BANK", [_elem("MY_BANK", "SUCCESS", 1)])
        assert rows[0].f0 == "MY_BANK"

    def test_field_types_are_correct(self):
        rows = _run_aggregator("ANZ", [_elem("ANZ", "SUCCESS", 100)])
        row = rows[0]
        assert isinstance(row.f0, str)    # bank_code
        assert isinstance(row.f1, int)    # total_transactions
        assert isinstance(row.f2, int)    # failed_transactions
        assert isinstance(row.f3, float)  # success_rate
        assert isinstance(row.f4, float)  # total_amount
        assert isinstance(row.f5, str)    # window_start

    def test_success_rate_is_between_zero_and_one(self):
        rows = _run_aggregator("ANZ", [
            _elem("ANZ", "SUCCESS", 10),
            _elem("ANZ", "FAILED",  20),
        ])
        assert 0.0 <= rows[0].f3 <= 1.0
