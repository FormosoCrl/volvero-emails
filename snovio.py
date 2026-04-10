import requests
import pandas as pd
import os
from dotenv import load_dotenv

# --- 1. CONFIGURACIÓN DEL ENTORNO ---
load_dotenv()

CLIENT_ID = os.getenv("SNOVIO_CLIENT_ID")
CLIENT_SECRET = os.getenv("SNOVIO_CLIENT_SECRET")

# --- 2. CONFIGURACIÓN GLOBAL ---
INPUT_FILE = "input_data.csv"
OUTPUT_FILE = "Snovio_Final_Results.csv"
EMAILS_PER_DOMAIN = 4


def get_access_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("API Credentials not found. Please check your .env file.")
    url = "https://api.snov.io/v1/oauth/access_token"
    payload = {"grant_type": "client_credentials", "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json().get("access_token")


def get_domain_emails(domain, token):
    url = "https://api.snov.io/v2/domain-emails-with-info"
    params = {"domain": domain, "type": "personal", "limit": EMAILS_PER_DOMAIN, "lastId": 0}
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, params=params, headers=headers, timeout=10)
    return response.json() if response.status_code == 200 else None


def main():
    print("--- Starting Snov.io Production Script ---")
    try:
        token = get_access_token()
    except Exception as e:
        print(f"Authentication Error: {e}");
        return

    try:
        if INPUT_FILE.endswith('.csv'):
            df = pd.read_csv(INPUT_FILE, header=0, encoding='latin1')
        else:
            df = pd.read_excel(INPUT_FILE)

        # MEJORA: Priorizamos 'website' o 'web' sobre 'company'
        cols = [c for c in df.columns if any(k in c.lower() for k in ['website', 'web', 'domain'])]
        if not cols:  # Si no hay 'web', buscamos 'company'
            cols = [c for c in df.columns if 'company' in c.lower()]

        domain_col = cols[0]
        print(f"Using column: '{domain_col}' for sourcing.\n")
    except Exception as e:
        print(f"Fatal Error: {e}");
        return

    results = []
    processed_count = 0

    for index, row in df.iterrows():
        raw_val = str(row[domain_col]).lower().strip()
        if raw_val == "nan" or not raw_val or raw_val == "none":
            continue

        # LIMPIEZA PROFUNDA: Extraemos el dominio real
        # Quitamos http, https, www y todo lo que vaya después de la primera barra /
        domain = raw_val.split('//')[-1].split('/')[0].replace("www.", "")

        # Si el dominio resultante tiene espacios o no tiene un punto, no es un dominio válido
        if " " in domain or "." not in domain:
            print(f"Skipping: '{domain}' (Not a valid domain format)")
            continue

        try:
            print(f"Processing: {domain}...")
            data = get_domain_emails(domain, token)
        except Exception as e:
            print(f"  > [!] Connection Error on {domain}. Skipping...");
            continue

        if data and data.get("emails"):
            for email_data in data["emails"]:
                results.append({
                    "Company": domain,
                    "First Name": email_data.get("firstName", ""),
                    "Last Name": email_data.get("lastName", ""),
                    "Position": email_data.get("position", "N/A"),
                    "Email": email_data.get("email", ""),
                    "Status": email_data.get("status", "")
                })
                print(f"  > Contact Found: {email_data.get('email')}")
        else:
            print(f"  > No contacts found.")
        processed_count += 1

    print("\n" + "=" * 40)
    print(f"EXECUTION SUMMARY\nProcessed: {processed_count}\nFound: {len(results)}")
    print("=" * 40)

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"\nResults successfully saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()