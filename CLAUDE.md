# Claude Instructions — volvero-emails

## Environment

This project uses a **conda virtual environment** named `volvero-emails`.

- Always run scripts with: `conda run -n volvero-emails python scraper.py`
- Or activate first: `conda activate volvero-emails`, then `python scraper.py`
- Do NOT use the system Python or pip directly.

### First-time setup on a new machine

```bash
conda env create -f environment.yml
conda activate volvero-emails
python -m playwright install chromium   # required — browser binaries are machine-local
```

### Re-sync deps after environment.yml changes

```bash
conda env update -f environment.yml --prune
```

## Known Issues / Fixes Applied

- `np.array_split(df, N)` returns `numpy.ndarray` objects, not DataFrames — replaced with `df.iloc`-based split in `scraper.py`.
- Playwright browser binaries must be installed per machine (`python -m playwright install chromium`). They are NOT part of the conda env package list.
- `conda run` prints pydantic warnings from the base env — these are harmless and unrelated to this project.

## Project Structure

```
scraper.py                  # main entry point
src/
  worker.py                 # multiprocessing worker
  page_scraper.py           # Playwright page scraping
  email_utils.py            # email extraction + filtering
  url_utils.py              # URL normalization
output_summury_detail/      # per-status CSV splits + summary.txt
environment.yml             # conda env definition
requirements.txt            # pip deps (kept for reference)
```

## Running

```bash
conda activate volvero-emails
python scraper.py
# Output: output_with_emails.csv
```
