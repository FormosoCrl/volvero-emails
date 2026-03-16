import logging
from typing import List, Optional
from urllib.parse import urljoin, urlparse

from src.email_utils import extract_emails

CONTACT_KEYWORDS = [
    'contact', 'about', 'team', 'people', 'reach',
    'hello', 'connect', 'imprint', 'impressum'
]
PAGE_TIMEOUT = 15000  # ms
MAX_CONTACT_PAGES = 5


def find_contact_links(hrefs: List[str], base_url: str) -> List[str]:
    """From a list of hrefs, return up to MAX_CONTACT_PAGES absolute URLs
    that contain a contact keyword and stay on the same domain."""
    base_domain = urlparse(base_url).netloc
    results = []
    for href in hrefs:
        if not href or href.startswith('mailto:') or href.startswith('javascript:'):
            continue
        if href.startswith('//'):
            href = 'https:' + href
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.netloc != base_domain:
            continue
        path_lower = parsed.path.lower()
        if any(kw in path_lower for kw in CONTACT_KEYWORDS):
            if absolute not in results:
                results.append(absolute)
        if len(results) >= MAX_CONTACT_PAGES:
            break
    return results


def scrape_page(page, url: str, worker_id: int) -> Optional[str]:
    """Load a URL and return page HTML, or None on failure."""
    response = page.goto(url, timeout=PAGE_TIMEOUT, wait_until='domcontentloaded')
    if response and response.status >= 400:
        logging.error(f"[Worker-{worker_id}] HTTP {response.status} on {url}")
        return None
    return page.content()


def scrape_site(browser, url: str, worker_id: int, row_num: int, chunk_size: int) -> str:
    """
    Full scrape of one site. Returns the emails string for the CSV cell.
    Handles homepage + contact page fallback.
    Does NOT handle retries — caller handles that.
    """
    log = f"[Worker-{worker_id}]"
    logging.info(f"{log} Processing: {url} (row {row_num}/{chunk_size})")

    page = browser.new_page()
    try:
        html = scrape_page(page, url, worker_id)
        if html is None:
            return "ERROR - failed to load page"

        final_url = page.url
        if final_url != url:
            logging.info(f"{log} Redirected to: {final_url}")

        emails = extract_emails(html)
        logging.info(f"{log} Homepage: {len(emails)} email(s) found")

        if not emails:
            hrefs = page.eval_on_selector_all('a[href]', 'els => els.map(e => e.getAttribute("href"))')
            contact_urls = find_contact_links(hrefs, final_url)
            if contact_urls:
                logging.info(f"{log} Contact pages found: {', '.join(contact_urls)}")
            for contact_url in contact_urls:
                try:
                    contact_html = scrape_page(page, contact_url, worker_id)
                    if contact_html:
                        new_emails = extract_emails(contact_html)
                        logging.info(f"{log} {contact_url} → {len(new_emails)} email(s)")
                        emails.extend(e for e in new_emails if e not in emails)
                except Exception as e:
                    logging.error(f"{log} Failed to load contact page {contact_url}: {e}")

        if emails:
            result = "; ".join(emails)
            logging.info(f"{log} Done: {result}")
            return result
        else:
            logging.info(f"{log} NOT_FOUND: {url}")
            return "NOT_FOUND"
    finally:
        page.close()
