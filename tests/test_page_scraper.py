import pytest
from src.page_scraper import find_contact_links

BASE_URL = "https://acme.com"

def test_find_contact_links_relative():
    links = ["/contact", "/about", "/pricing"]
    result = find_contact_links(links, BASE_URL)
    assert "https://acme.com/contact" in result
    assert "https://acme.com/about" in result
    assert "https://acme.com/pricing" not in result

def test_find_contact_links_absolute():
    links = ["https://acme.com/team", "https://other.com/contact"]
    result = find_contact_links(links, BASE_URL)
    assert "https://acme.com/team" in result
    assert "https://other.com/contact" not in result

def test_find_contact_links_protocol_relative():
    links = ["//acme.com/imprint"]
    result = find_contact_links(links, BASE_URL)
    assert "https://acme.com/imprint" in result

def test_find_contact_links_max_5():
    links = [f"/contact{i}" for i in range(10)]
    result = find_contact_links(links, BASE_URL)
    assert len(result) <= 5

def test_find_contact_links_empty():
    assert find_contact_links([], BASE_URL) == []

def test_find_contact_links_no_matches():
    links = ["/pricing", "/faq", "/legal"]
    assert find_contact_links(links, BASE_URL) == []
