# volvero-emails

Scrapes company websites from a CSV of EU investors and extracts contact emails using Playwright (headless Chromium). Runs with 10 parallel workers via multiprocessing.

## Setup

Requires [Conda](https://docs.conda.io/en/latest/).

```bash
# 1. Create the environment
conda env create -f environment.yml

# 2. Activate it
conda activate volvero-emails

# 3. Install Playwright browser (once per machine)
python -m playwright install chromium
```

> The browser binaries are machine-local and are NOT stored in the conda env. Re-run step 3 on any new machine.

## Usage

```bash
conda activate volvero-emails
python scraper.py
```

Input: `List of EU Investors.xlsx - VC FINAL for database.csv`
Output: `output_with_emails.csv`

A per-status breakdown is also written to `output_summury_detail/`:

| File | Content |
|------|---------|
| `emails_found_N.csv` | Rows where emails were extracted |
| `not_found_N.csv` | Sites that loaded but had no email |
| `error_N.csv` | SSL / DNS / HTTP 4xx failures |
| `no_url_N.csv` | Rows with no website in source data |
| `summary.txt` | Counts table |

## Update dependencies

```bash
conda env update -f environment.yml --prune
```
