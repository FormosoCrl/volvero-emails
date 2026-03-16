import time
import logging
from typing import List, Tuple

from playwright.sync_api import sync_playwright
from src.page_scraper import scrape_site
from src.url_utils import normalize_url

MAX_RETRIES = 2
RETRY_DELAY = 3  # seconds


def _format_error(e: Exception) -> str:
    reason = str(e).replace(';', ',').replace(':', '-')[:80]
    return f"ERROR - {reason}"


def worker_func(args: Tuple) -> List[dict]:
    """
    Worker process entry point.
    args = (worker_id, chunk_df, queue)
    Returns list of {index, emails} dicts.
    Also pushes each result to queue for real-time progress tracking.
    """
    worker_id, chunk_df, queue = args
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        chunk_size = len(chunk_df)

        for i, (idx, row) in enumerate(chunk_df.iterrows(), start=1):
            raw_url = row.get('Website')
            url = normalize_url(raw_url)

            if url is None:
                logging.info(f"[Worker-{worker_id}] Skipping row {idx}: NO_URL")
                result = {'index': idx, 'emails': 'NO_URL'}
                results.append(result)
                queue.put(result)
                continue

            emails_value = None
            # attempt 0 = first try, attempts 1..MAX_RETRIES = retries
            for attempt in range(MAX_RETRIES + 1):
                try:
                    emails_value = scrape_site(browser, url, worker_id, i, chunk_size)
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        logging.error(
                            f"[Worker-{worker_id}] ERROR on {url} "
                            f"(retry {attempt + 1}/{MAX_RETRIES}): {e}"
                        )
                        time.sleep(RETRY_DELAY)
                    else:
                        logging.error(
                            f"[Worker-{worker_id}] SKIP {url} after {MAX_RETRIES} retries: {e}"
                        )
                        emails_value = _format_error(e)

            result = {'index': idx, 'emails': emails_value}
            results.append(result)
            queue.put(result)

        browser.close()

    return results
