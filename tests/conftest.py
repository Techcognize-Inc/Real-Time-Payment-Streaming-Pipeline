# Shared configuration for pytest

import sys
from pathlib import Path

# Add project folders to Python path so tests can import modules
PROJECT_ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(PROJECT_ROOT / "airflow" / "dags"))
sys.path.append(str(PROJECT_ROOT / "airflow" / "dags" / "producer"))