import logging
import multiprocessing
import threading
import time

import numpy as np
import pandas as pd

from src.worker import worker_func

INPUT_FILE = "List of EU Investors.xlsx - VC FINAL for database.csv"
OUTPUT_FILE = "output_with_emails.csv"
NUM_WORKERS = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)


def progress_reader(queue, total_rows):
    """Runs in reader thread. Drains queue, tracks progress, prints summaries."""
    counts = {'found': 0, 'not_found': 0, 'error': 0, 'no_url': 0, 'total': 0}
    results = {}

    while True:
        item = queue.get()
        if item is None:  # sentinel
            break

        val = item['emails']
        results[item['index']] = val
        counts['total'] += 1

        if val == 'NO_URL':
            counts['no_url'] += 1
        elif val == 'NOT_FOUND':
            counts['not_found'] += 1
        elif val.startswith('ERROR'):
            counts['error'] += 1
        else:
            counts['found'] += 1

        if counts['total'] % 50 == 0:
            logging.info(
                f"[PROGRESS] {counts['total']}/{total_rows} processed | "
                f"{counts['found']} emails found | "
                f"{counts['error']} errors | "
                f"{counts['not_found']} not found"
            )

    return results, counts


def main():
    start_time = time.time()

    # Load CSV — row 0 is category label, row 1 is actual header
    df = pd.read_csv(INPUT_FILE, header=1, dtype=str)
    logging.info(f"[INFO] Loaded {len(df)} rows from {INPUT_FILE}")

    # Split into chunks
    chunks = np.array_split(df, NUM_WORKERS)

    # Shared queue
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    total_rows = len(df)

    # Start progress reader thread
    results_store = {}
    final_counts = {}

    def reader_target():
        nonlocal results_store, final_counts
        results_store, final_counts = progress_reader(queue, total_rows)

    reader_thread = threading.Thread(target=reader_target)
    reader_thread.start()

    # Launch worker pool with spawn context (Windows-safe)
    ctx = multiprocessing.get_context('spawn')
    with ctx.Pool(processes=NUM_WORKERS) as pool:
        args = [(i, chunk, queue) for i, chunk in enumerate(chunks, start=1)]
        pool.map(worker_func, args)

    # Send sentinel to stop reader
    queue.put(None)
    reader_thread.join()

    # Merge results back into dataframe
    df['emails'] = df.index.map(lambda i: results_store.get(i, 'ERROR - missing result'))

    # Write output CSV with UTF-8 BOM
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

    elapsed = round((time.time() - start_time) / 60, 1)
    logging.info(
        f"\n[DONE] {total_rows} rows processed in {elapsed} min\n"
        f"Emails found: {final_counts.get('found', 0)} | "
        f"NOT_FOUND: {final_counts.get('not_found', 0)} | "
        f"ERROR: {final_counts.get('error', 0)} | "
        f"NO_URL: {final_counts.get('no_url', 0)}\n"
        f"Output saved to: {OUTPUT_FILE}"
    )


if __name__ == '__main__':
    main()
