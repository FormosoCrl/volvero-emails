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




## Snov.io API Extractor (New Feature)

Uses the Snov.io API v2 to find and verify personal emails associated with specific company domains. Includes automatic URL normalization and built-in credit management.

### Configuration

To use the Snov.io extraction script, you must configure your API credentials securely:

```bash
# 1. Install python-dotenv (if not already in your conda environment)
pip install python-dotenv

# 2. Duplicate the template file to create your local copy
cp .env.example .env

# 3. Open the .env file in your editor and insert your CLIENT_ID and CLIENT_SECRET
```
> Note: The .env file is git-ignored to prevent credential leaks.

### Usage

```bash
conda activate volvero-emails
python snovio.py
```
Input: input_data.csv (Automatically detects columns containing "website" or "domain")

Output: Snovio_Final_Results.csv (Contains verified leads with First Name, Last Name, Position, and Email)
