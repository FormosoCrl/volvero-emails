"""
=========================================================================================
VOLVERO BATCH ENRICHER - FULL EXCEL EDITION (.XLSX) - HYBRID SNIPER v2
=========================================================================================
"""

import os
import time
import threading
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from google import genai

# --- 1. CONFIGURACIÓN ---
load_dotenv()

INPUT_FILE = "new_emails_cleaned_by_Alaa.xlsx"
OUTPUT_FILE = "new_emails_cleaned_by_Alaa_ENRICHED.xlsx"
MAX_WORKERS = 5
CHECKPOINT_EVERY = 50

api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    print("❌ ERROR: No se encuentra la GEMINI_API_KEY en el archivo .env")
    exit(1)

client_google = genai.Client(api_key=api_key)
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

snov_cache = {"token": None, "expiry": 0}
snov_lock = threading.Lock()


# --- 2. RATE LIMITER GLOBAL ---

class RateLimiter:
    """Garantiza un máximo de N llamadas por minuto globalmente entre todos los hilos."""
    def __init__(self, calls_per_minute: int):
        self._interval = 60.0 / calls_per_minute
        self._lock = threading.Lock()
        self._last_call = 0.0

    def acquire(self):
        with self._lock:
            now = time.time()
            wait = self._interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.time()

gemini_limiter = RateLimiter(calls_per_minute=14)


# --- 3. SNOV.IO ENGINE (MODO ESTRICTO) ---

def get_snovio_token():
    with snov_lock:
        if snov_cache["token"] and time.time() < snov_cache["expiry"]:
            return snov_cache["token"]

        cid = os.getenv("SNOVIO_CLIENT_ID")
        sec = os.getenv("SNOVIO_CLIENT_SECRET")
        if not cid or not sec:
            return None

        try:
            res = requests.post(
                "https://api.snov.io/v1/oauth/access_token",
                data={"grant_type": "client_credentials", "client_id": cid, "client_secret": sec},
                timeout=10
            )
            res.raise_for_status()
            token = res.json().get("access_token")
            if token:
                snov_cache["token"] = token
                snov_cache["expiry"] = time.time() + 2700  # 45 min de margen
            return token
        except Exception as e:
            print(f"⚠️ Error Snov.io token: {e}")
            return None


def fetch_snovio_by_person(full_name, domain, token):
    parts = full_name.split(" ", 1)
    payload = {
        "firstName": parts[0],
        "lastName": parts[1] if len(parts) > 1 else "",
        "domain": domain
    }
    try:
        res = requests.post(
            "https://api.snov.io/v1/get-emails-from-names",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=10
        )
        data = res.json()
        email_val = (data.get("data") or {}).get("email")
        if data.get("success") and email_val:
            return email_val
    except:
        pass
    return None


def fetch_snovio_by_domain(domain, token):
    url = f"https://api.snov.io/v2/domain-emails-with-info?domain={domain}&type=personal&limit=3"
    try:
        res = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        emails = [e["email"] for e in res.json().get("emails", []) if e.get("email")]
        return "; ".join(emails) if emails else None
    except:
        pass
    return None


# --- 4. AI AGENTS (CEREBRO HÍBRIDO CLEARBIT + GEMINI) ---

def find_domain_with_ai(company_name, retries=2):
    if not company_name or str(company_name).lower() == "nan":
        return None

    # FASE 1: CLEARBIT (instantáneo, sin rate limit)
    try:
        res = requests.get(
            f"https://autocomplete.clearbit.com/v1/companies/suggest?query={company_name}",
            timeout=5
        )
        data = res.json()
        if data and len(data) > 0:
            return data[0]["domain"]
    except:
        pass

    # FASE 2: GEMINI (con rate limiter global)
    prompt = (
        f"You are an expert Data Entry Agent. What is the official website domain for the company/investor '{company_name}'?\n"
        f"Return ONLY the root domain (e.g., 'acme.com'). No URLs, no paths, no 'https://'.\n"
        f"If you do not know it with absolute certainty, return exactly: NONE."
    )

    for attempt in range(retries + 1):
        try:
            gemini_limiter.acquire()
            response = client_google.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            domain = response.text.strip().lower()
            if "none" in domain or " " in domain:
                return None
            return domain.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
        except:
            if attempt < retries:
                time.sleep(5)
    return None


def find_linkedin_with_ai(name, company_name, retries=1):
    prompt = (
        f"You are a researcher. What is the exact LinkedIn profile URL for {name} who works at {company_name}?\n"
        f"Return ONLY the URL starting with https://www.linkedin.com/in/. If you don't know it with high confidence, return exactly: NONE."
    )

    for attempt in range(retries + 1):
        try:
            gemini_limiter.acquire()
            response = client_google.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            url = response.text.strip()
            if url.startswith("https://www.linkedin.com/in/") or url.startswith("https://linkedin.com/in/"):
                return url
            return None
        except:
            if attempt < retries:
                time.sleep(5)
    return None


# --- 5. ORQUESTADOR DE FILAS ---

def process_row(index, row, token):
    status = str(row.get("Status", "")).strip()
    if status != "To find":
        return index, row

    is_person = str(row.get("Category", "")).strip().lower() == "person"
    name = str(row.get("Name", "")).strip()
    company = str(row.get("Organization", "")).strip() if is_person else name
    if not company:
        company = name

    # 1. Busca dominio
    domain = str(row.get("Websites", "")).strip()
    if not domain:
        domain = find_domain_with_ai(company)
        if domain:
            row["Websites"] = domain

    # 2. Snov.io (modo estricto)
    emails_found = None
    if domain and token:
        if is_person and name:
            emails_found = fetch_snovio_by_person(name, domain, token)
        else:
            emails_found = fetch_snovio_by_domain(domain, token)

    # 3. LinkedIn (solo personas, con validación)
    linkedin = str(row.get("LinkedIn", "")).strip()
    if is_person and not linkedin:
        ln_url = find_linkedin_with_ai(name, company)
        if ln_url:
            row["LinkedIn"] = ln_url

    # 4. Estado final
    if emails_found:
        row["Emails"] = emails_found
        row["Status"] = "Enriched - Snovio"
    elif domain:
        row["Status"] = "Not Found in Snovio (Domain Found)"
    else:
        row["Status"] = "Not Found (No Domain)"

    return index, row


# --- 6. CHECKPOINT ---

def save_checkpoint(excel_data, output_file):
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, df in excel_data.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


# --- 7. MAIN ---

def main():
    print(f"📥 Cargando Excel: {INPUT_FILE}...")
    try:
        excel_data = pd.read_excel(INPUT_FILE, sheet_name=None, dtype=str)
    except Exception as e:
        print(f"❌ Error crítico leyendo el Excel: {e}")
        return

    token = get_snovio_token()
    if not token:
        print("⚠️ Sin token de Snov.io. Revisa tu archivo .env.")

    start_time = time.time()

    for sheet_name, df in excel_data.items():
        df = df.fillna("")

        if "Status" not in df.columns or "Name" not in df.columns:
            print(f"⏩ Pestaña ignorada: '{sheet_name}'")
            continue

        print(f"\n🔍 Procesando pestaña: '{sheet_name}'")
        to_process = df[df["Status"].str.strip() == "To find"]
        total = len(to_process)
        print(f"📊 Filas a enriquecer: {total}")

        if total == 0:
            continue

        results = {}
        processed_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_row, idx, row.copy(), token): idx
                for idx, row in to_process.iterrows()
            }

            for future in as_completed(futures):
                idx, updated_row = future.result()
                results[idx] = updated_row
                processed_count += 1

                if processed_count % 10 == 0 or processed_count == total:
                    print(f"⚙️ Progreso '{sheet_name}': {processed_count}/{total}")

                if processed_count % CHECKPOINT_EVERY == 0:
                    for i, r in results.items():
                        df.loc[i] = r
                    excel_data[sheet_name] = df
                    save_checkpoint(excel_data, OUTPUT_FILE)
                    print(f"💾 Checkpoint guardado ({processed_count} filas)")

        # Aplicar todos los resultados en el hilo principal
        for idx, updated_row in results.items():
            df.loc[idx] = updated_row
        excel_data[sheet_name] = df

    print(f"\n💾 Guardando archivo final: {OUTPUT_FILE}...")
    save_checkpoint(excel_data, OUTPUT_FILE)

    elapsed = round((time.time() - start_time) / 60, 1)
    print(f"🚀 ¡Completado en {elapsed} minutos!")


if __name__ == "__main__":
    main()