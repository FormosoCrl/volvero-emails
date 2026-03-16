import pytest
from src.email_utils import extract_emails

def test_finds_plain_email():
    html = '<p>Contact us at info@acme.com for more info.</p>'
    assert extract_emails(html) == ["info@acme.com"]

def test_finds_multiple_emails():
    html = 'info@acme.com and john@acme.com'
    assert set(extract_emails(html)) == {"info@acme.com", "john@acme.com"}

def test_deduplicates_case_insensitive():
    html = 'INFO@acme.com info@acme.com INFO@ACME.COM'
    assert extract_emails(html) == ["info@acme.com"]

def test_filters_image_extension():
    html = 'src="logo@2x.png"'
    assert extract_emails(html) == []

def test_filters_js_extension():
    html = 'file@bundle.js'
    assert extract_emails(html) == []

def test_filters_css_extension():
    html = 'file@styles.css'
    assert extract_emails(html) == []

def test_filters_css_artifact_charset():
    html = '@charset "UTF-8";'
    assert extract_emails(html) == []

def test_filters_css_artifact_keyframes():
    html = '@keyframes spin {}'
    assert extract_emails(html) == []

def test_filters_noreply():
    html = 'noreply@acme.com'
    assert extract_emails(html) == []

def test_filters_no_reply():
    html = 'no-reply@acme.com'
    assert extract_emails(html) == []

def test_filters_donotreply():
    html = 'donotreply@acme.com'
    assert extract_emails(html) == []

def test_filters_placeholder_domain():
    html = 'test@example.com and hello@yourdomain.com'
    assert extract_emails(html) == []

def test_filters_retina_suffix():
    html = 'image@2x'
    assert extract_emails(html) == []

def test_allows_legit_2x_domain():
    html = 'contact@2xgrowth.vc'
    assert extract_emails(html) == ["contact@2xgrowth.vc"]

def test_empty_html():
    assert extract_emails("") == []

def test_no_emails():
    assert extract_emails("<p>No emails here</p>") == []
