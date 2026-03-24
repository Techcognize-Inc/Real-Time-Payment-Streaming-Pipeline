# Shared configuration for pytest

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add project folders to Python path so tests can import modules
PROJECT_ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(PROJECT_ROOT / "airflow" / "dags"))
sys.path.append(str(PROJECT_ROOT / "airflow" / "dags" / "producer"))
sys.path.append(str(PROJECT_ROOT / "flink_job"))

# ---------------------------------------------------------------------------
# Stub base classes — Python must be able to *inherit* from these
# ---------------------------------------------------------------------------

class _ProcessFunction:
    """Stub so ParseEvent can inherit from ProcessFunction without pyflink."""


class _ProcessWindowFunction:
    """Stub so PaymentAggregator can inherit from ProcessWindowFunction."""


class _OutputTag:
    def __init__(self, tag_id, type_info=None):
        self.tag_id = tag_id


class _Row:
    """Minimal Row substitute that supports field attribute access (f0, f1 …)."""

    def __init__(self, *args):
        self._fields = args
        for i, v in enumerate(args):
            setattr(self, f"f{i}", v)

    def __iter__(self):
        return iter(self._fields)

    def __repr__(self):
        return f"Row({', '.join(repr(f) for f in self._fields)})"


# ---------------------------------------------------------------------------
# Build mock module objects and inject into sys.modules BEFORE any test
# imports the Flink source (which runs top-level code on import)
# ---------------------------------------------------------------------------

_mock_functions = MagicMock(name="pyflink.datastream.functions")
_mock_functions.ProcessFunction = _ProcessFunction
_mock_functions.ProcessWindowFunction = _ProcessWindowFunction

_mock_datastream = MagicMock(name="pyflink.datastream")
_mock_datastream.OutputTag = _OutputTag
_mock_datastream.functions = _mock_functions

_mock_common = MagicMock(name="pyflink.common")
_mock_common.Row = _Row

_PYFLINK_MOCKS = {
    "pyflink":                             MagicMock(name="pyflink"),
    "pyflink.datastream":                  _mock_datastream,
    "pyflink.datastream.functions":        _mock_functions,
    "pyflink.datastream.window":           MagicMock(name="pyflink.datastream.window"),
    "pyflink.datastream.connectors":       MagicMock(name="pyflink.datastream.connectors"),
    "pyflink.datastream.connectors.kafka": MagicMock(name="pyflink.datastream.connectors.kafka"),
    "pyflink.datastream.connectors.jdbc":  MagicMock(name="pyflink.datastream.connectors.jdbc"),
    "pyflink.common":                      _mock_common,
    "pyflink.common.typeinfo":             MagicMock(name="pyflink.common.typeinfo"),
    "pyflink.common.time":                 MagicMock(name="pyflink.common.time"),
    "pyflink.common.serialization":        MagicMock(name="pyflink.common.serialization"),
    "psycopg2":                            MagicMock(name="psycopg2"),
}

for _name, _mock in _PYFLINK_MOCKS.items():
    if _name not in sys.modules:
        sys.modules[_name] = _mock