import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _offline_mitre_dataset(tmp_path, monkeypatch):
    dataset_path = tmp_path / 'mitre_stub.json'
    dataset_path.write_text('{"objects": []}', encoding='utf-8')
    monkeypatch.setenv('MITRE_ATTACK_PATH', str(dataset_path))
