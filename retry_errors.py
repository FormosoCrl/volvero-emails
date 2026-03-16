import logging
import math
import multiprocessing
import os
import threading
import time

import pandas as pd

from src.worker import worker_func

ERROR_FILE = "output_summury_detail/error_145.csv"
MAIN_OUTPUT = "output_with_emails.csv"
SUMMARY_DIR = "output_summury_detail"
NUM_WORKERS = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)


def classify(val):
    if pd.isna(val) or str(val).strip() == '':
        return 'no_url'
    v = str(val).strip()
    if v == 'NO_URL':
        return 'no_url'
    elif v == 'NOT_FOUND':
        return 'not_found'
    elif v.startswith('ERROR'):
        return 'error'
    else:
        return 'emails_found'


def rebuild_summary(df):
    """Rebuild per-status CSVs (with index) and summary.txt from a full output dataframe."""
    df['_status'] = df['emails'].apply(classify)
    counts = df['_status'].value_counts()

    # Remove old status CSVs
    for f in os.listdir(SUMMARY_DIR):
        if f.endswith('.csv'):
            os.remove(os.path.join(SUMMARY_DIR, f))

    for status, count in counts.items():
        subset = df[df['_status'] == status].drop(columns=['_status'])
        filename = f'{status}_{count}.csv'
        # index=True preserves original row positions for correct retry merging
        subset.to_csv(os.path.join(SUMMARY_DIR, filename), index=True, encoding='utf-8-sig')
        logging.info(f"[SUMMARY] Written: {filename}")

    total = len(df)
    summary = (
        f"Scrape Summary — {total} rows total\n"
        f"=======================================================\n\n"
        f"| Status        | Count | % of Total |\n"
        f"|---------------|-------|------------|\n"
        f"| Emails found  | {counts.get('emails_found', 0):>5} | {counts.get('emails_found', 0)/total*100:>8.1f}%  |\n"
        f"| NOT_FOUND     | {counts.get('not_found', 0):>5} | {counts.get('not_found', 0)/total*100:>8.1f}%  |\n"
        f"| ERROR         | {counts.get('error', 0):>5} | {counts.get('error', 0)/total*100:>8.1f}%  |\n"
        f"| NO_URL        | {counts.get('no_url', 0):>5} | {counts.get('no_url', 0)/total*100:>8.1f}%  |\n"
        f"| TOTAL         | {total:>5} | {'100.0%':>9}  |\n\n"
        f"Files\n"
        f"-----\n"
        f"emails_found_{counts.get('emails_found', 0)}.csv  — rows where one or more emails were extracted\n"
        f"not_found_{counts.get('not_found', 0)}.csv     — rows where the site loaded but no email was found\n"
        f"error_{counts.get('error', 0)}.csv         — rows that failed (SSL error, DNS failure, HTTP 4xx, etc.)\n"
        f"no_url_{counts.get('no_url', 0)}.csv         — rows with no website URL in the source data\n"
    )
    with open(os.path.join(SUMMARY_DIR, 'summary.txt'), 'w', encoding='utf-8') as f:
        f.write(summary)

    df.drop(columns=['_status'], inplace=True)
    return counts


def progress_reader(queue, total_rows):
    counts = {'found': 0, 'not_found': 0, 'error': 0, 'no_url': 0, 'total': 0}
    results = {}

    while True:
        item = queue.get()
        if item is None:
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

        if counts['total'] % 20 == 0:
            logging.info(
                f"[PROGRESS] {counts['total']}/{total_rows} processed | "
                f"{counts['found']} emails found | "
                f"{counts['error']} errors | "
                f"{counts['not_found']} not found"
            )

    return results, counts


def main():
    start_time = time.time()

    # index_col=0 restores the original row positions from the main output
    error_df = pd.read_csv(ERROR_FILE, dtype=str, encoding='utf-8-sig', index_col=0)
    logging.info(f"[INFO] Retrying {len(error_df)} errored rows from {ERROR_FILE}")
    logging.info(f"[INFO] Index range: {error_df.index.min()} – {error_df.index.max()}")

    chunk_size = math.ceil(len(error_df) / NUM_WORKERS)
    chunks = [error_df.iloc[i:i + chunk_size] for i in range(0, len(error_df), chunk_size)]

    manager = multiprocessing.Manager()
    queue = manager.Queue()
    total_rows = len(error_df)

    results_store = {}
    final_counts = {}

    def reader_target():
        nonlocal results_store, final_counts
        results_store, final_counts = progress_reader(queue, total_rows)

    reader_thread = threading.Thread(target=reader_target)
    reader_thread.start()

    ctx = multiprocessing.get_context('spawn')
    with ctx.Pool(processes=NUM_WORKERS) as pool:
        args = [(i, chunk, queue) for i, chunk in enumerate(chunks, start=1)]
        pool.map(worker_func, args)

    queue.put(None)
    reader_thread.join()

    # results_store keys are the original main_df indices — safe to update directly
    error_df['emails'] = error_df.index.map(lambda i: results_store.get(i, 'ERROR - missing result'))

    # Merge into main output by index — now correctly aligned
    main_df = pd.read_csv(MAIN_OUTPUT, dtype=str, encoding='utf-8-sig')
    main_df.update(error_df[['emails']])
    main_df.to_csv(MAIN_OUTPUT, index=False, encoding='utf-8-sig')
    logging.info(f"[INFO] Merged results into {MAIN_OUTPUT}")

    counts = rebuild_summary(main_df)

    elapsed = round((time.time() - start_time) / 60, 1)
    logging.info(
        f"\n[DONE] {total_rows} rows retried in {elapsed} min\n"
        f"Newly found: {final_counts.get('found', 0)} | "
        f"Still erroring: {final_counts.get('error', 0)} | "
        f"NOT_FOUND: {final_counts.get('not_found', 0)}\n"
        f"Summary CSVs updated in: {SUMMARY_DIR}/"
    )


if __name__ == '__main__':
    main()
