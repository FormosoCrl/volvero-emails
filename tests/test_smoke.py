import subprocess
import pytest

@pytest.mark.slow
def test_smoke_run():
    """Verify core modules work together without a live browser."""
    result = subprocess.run(
        ['python', '-c', '''
import pandas as pd
import numpy as np
from src.url_utils import normalize_url
from src.email_utils import extract_emails

df = pd.read_csv("sample_test.csv", header=1, dtype=str)
assert "Website" in df.columns, f"columns: {list(df.columns)}"
assert len(df) == 5, f"expected 5 rows, got {len(df)}"

assert normalize_url("www.kimaventures.com") == "https://www.kimaventures.com"
assert normalize_url("") is None

emails = extract_emails("Contact: info@test.com and sales@test.com")
assert "info@test.com" in emails
assert "sales@test.com" in emails

print("SMOKE TEST PASSED")
'''],
        capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "SMOKE TEST PASSED" in result.stdout
