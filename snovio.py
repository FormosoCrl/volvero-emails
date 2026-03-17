import requests
import pandas as pd
import os
from dotenv import load_dotenv

# --- 1. ENVIRONMENT CONFIGURATION ---
# Load environment variables from a .env file for security
load_dotenv()

# API credentials are now pulled from environment variables to avoid leaking secrets
CLIENT_ID = os.getenv("SNOVIO_CLIENT_ID")
CLIENT_SECRET = os.getenv("SNOVIO_CLIENT_SECRET")

# --- 2. GLOBAL CONFIGURATION ---
INPUT_FILE = "input_data.csv"  # Source file containing investor/company websites
OUTPUT_FILE = "Snovio_Final_Results.csv"  # Target file for verified leads
EMAILS_PER_DOMAIN = 4  # Max contacts to retrieve per domain


def get_access_token():
    """
    Authenticates with Snov.io API to obtain a temporary OAuth2 access token.
    Raises an error if credentials are missing in the environment.
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("API Credentials not found. Please check your .env file.")

    url = "https://api.snov.io/v1/oauth/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json().get("access_token")


def get_domain_emails(domain, token):
    """
    Queries the Snov.io API v2 for personal email addresses associated with a specific domain.
    """
    url = "https://api.snov.io/v2/domain-emails-with-info"
    params = {
        "domain": domain,
        "type": "personal",
        "limit": EMAILS_PER_DOMAIN,
        "lastId": 0
    }
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, params=params, headers=headers)
    return response.json() if response.status_code == 200 else None


def main():
    print("--- Starting Snov.io Production Script ---")

    try:
        token = get_access_token()
    except Exception as e:
        print(f"Authentication Error: {e}")
        return

    # Credit management for the current session.
    # Set this value according to your available Snov.io credits.
    available_credits = 1000
    print(f"Current session credit limit: {available_credits}\n")

    # Load input data with encoding handling for Excel-exported CSVs
    try:
        df = pd.read_csv(INPUT_FILE, header=0, encoding='latin1')
        domain_col = [c for c in df.columns if 'web' in c.lower() or 'domain' in c.lower()][0]
    except Exception as e:
        print(f"Fatal Error: Could not read or parse the CSV file. Details: {e}")
        return

    results = []
    last_company = "None"
    last_user = "None"
    processed_count = 0

    for index, row in df.iterrows():
        # URL NORMALIZATION: Extracts the root domain
        raw_url = str(row[domain_col]).lower().strip()
        domain = raw_url.replace("https://", "").replace("http://", "").replace("www.", "").split('/')[0]

        if available_credits <= 0:
            print(f"\n{domain}: Credit limit reached for this session.")
            break

        print(f"Processing: {domain}...")
        data = get_domain_emails(domain, token)

        if data and data.get("emails"):
            emails_found = data["emails"]
            available_credits -= (len(emails_found) * 2)

            for email_data in emails_found:
                first = email_data.get("firstName", "")
                last = email_data.get("lastName", "")
                full_name = f"{first} {last}".strip()
                email = email_data.get("email", "")

                print(f"  > Contact Found: {email}")

                results.append({
                    "Company": domain,
                    "First Name": first,
                    "Last Name": last,
                    "Position": email_data.get("position", "N/A"),
                    "Email": email,
                    "Status": email_data.get("status", "")
                })
                last_user = f"{full_name} ({email})"
        else:
            print(f"  > No contacts found for this domain.")

        last_company = domain
        processed_count += 1

    # --- SESSION SUMMARY REPORT ---
    print("\n" + "=" * 40)
    print("EXECUTION SUMMARY")
    print(f"Total companies processed: {processed_count} / {len(df)}")
    print(f"Remaining records in file: {len(df) - processed_count}")
    print(f"Last record processed: {last_company}")
    print(f"Last contact retrieved: {last_user}")
    print("=" * 40)

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"\nResults successfully saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()