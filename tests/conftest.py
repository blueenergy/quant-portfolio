import os
import sys

import pytest

# Make the repo root importable when pytest is run from any directory.
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


@pytest.fixture
def base_date():
    """测试用基准日期"""
    return "20250910"
