import pytest
from src.url_utils import normalize_url

def test_bare_domain():
    assert normalize_url("500.co") == "https://500.co"

def test_www_domain():
    assert normalize_url("www.kimaventures.com") == "https://www.kimaventures.com"

def test_already_https():
    assert normalize_url("https://example.com") == "https://example.com"

def test_http_kept():
    assert normalize_url("http://example.com") == "http://example.com"

def test_empty_string():
    assert normalize_url("") is None

def test_none_value():
    assert normalize_url(None) is None

def test_nan_value():
    import math
    assert normalize_url(float("nan")) is None

def test_whitespace_only():
    assert normalize_url("   ") is None

def test_strips_whitespace():
    assert normalize_url("  https://example.com  ") == "https://example.com"
