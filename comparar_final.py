import pandas as pd
from urllib.parse import urlparse

# --- CONFIGURATION ---
FILE_ORIGINAL = 'input_data.csv'
FILE_PREVIOUS = 'results alaa.csv'
FILE_SNOVIO = 'Snovio_Final_Results.csv'
FILE_OUTPUT = 'results_stats_check.csv'


def clean_domain(url):
    if pd.isna(url) or str(url).lower().strip() in ['no website', 'nan', '']: return None
    url = str(url).lower().strip()
    if not url.startswith(('http://', 'https://')): url = 'http://' + url
    try:
        domain = urlparse(url).netloc
        return domain.replace('www.', '').strip()
    except:
        return None


def is_generic(email):
    if pd.isna(email): return False
    bad_list = ['info', 'contact', 'admin', 'support', 'hello', 'sales', 'office',
                'mail', 'marketing', 'enquiries', 'help', 'team', 'jobs', 'hr', 'press']
    prefix = str(email).lower().split('@')[0].strip()
    return prefix in bad_list


def safe_load(filename):
    for enc in ['utf-8-sig', 'latin1', 'cp1252']:
        try:
            df = pd.read_csv(filename, encoding=enc)
            df.columns = df.columns.str.strip().str.replace('ï»¿', '')
            return df
        except:
            continue
    raise Exception(f"Could not read {filename}")


try:
    orig = safe_load(FILE_ORIGINAL)
    prev = safe_load(FILE_PREVIOUS)
    snov = safe_load(FILE_SNOVIO)

    # Normalize Domains
    orig['dom'] = orig['Website'].apply(clean_domain)
    prev['dom'] = prev['Website'].apply(clean_domain)
    snov_col = 'Company' if 'Company' in snov.columns else snov.columns[0]
    snov['dom'] = snov[snov_col].apply(lambda x: str(x).lower().replace('www.', '').strip())

    # --- PREVIOUS VERSION ANALYSIS ---
    prev_exp = prev.assign(emails=prev['emails'].str.split(';')).explode('emails')
    prev_exp['emails'] = prev_exp['emails'].str.strip()
    prev_exp['is_gen'] = prev_exp['emails'].apply(is_generic)

    total_prev_emails = len(prev_exp.dropna(subset=['emails']))
    gen_prev_emails = int(prev_exp['is_gen'].sum())
    pers_prev_emails = total_prev_emails - gen_prev_emails

    a_group = prev_exp.dropna(subset=['emails']).groupby('dom').agg(
        Total_Prev=('emails', 'count'),
        Gen_Prev=('is_gen', 'sum'),
        Emails_List_Prev=('emails', lambda x: '; '.join(x))
    ).reset_index()

    # --- SNOVIO ANALYSIS ---
    snov['is_gen'] = snov['Email'].apply(is_generic)
    total_snov_emails = len(snov)
    gen_snov_emails = int(snov['is_gen'].sum())
    pers_snov_emails = total_snov_emails - gen_snov_emails

    s_group = snov.groupby('dom').agg(
        Total_Snovio=('Email', 'count'),
        Gen_Snovio=('is_gen', 'sum'),
        Emails_List_Snovio=('Email', lambda x: '; '.join(x))
    ).reset_index()

    # --- MERGE ---
    final = orig[['Firm name', 'Website', 'dom']].copy()
    final = final.merge(a_group, on='dom', how='left').merge(s_group, on='dom', how='left').fillna(0)


    def classify(row):
        if row['Total_Snovio'] > 0 and row['Total_Prev'] > 0: return 'Both Found'
        if row['Total_Snovio'] > 0: return 'Snovio Only'
        if row['Total_Prev'] > 0: return 'Previous Only'
        return 'Both Missed'


    final['Comparison_Status'] = final.apply(classify, axis=1)

    # Useful columns for CSV
    final['Personal_Snovio'] = final['Total_Snovio'] - final['Gen_Snovio']
    final['Personal_Prev'] = final['Total_Prev'] - final['Gen_Prev']
    final['Emails_Missed_By_Snovio'] = final.apply(
        lambda r: r['Emails_List_Prev'] if r['Comparison_Status'] == 'Previous Only' else '', axis=1
    )

    final.to_csv(FILE_OUTPUT, index=False, encoding='utf-8-sig')

    # Statistics
    total_rows = len(final)
    s_only = int((final['Comparison_Status'] == 'Snovio Only').sum())
    p_only = int((final['Comparison_Status'] == 'Previous Only').sum())
    both_f = int((final['Comparison_Status'] == 'Both Found').sum())
    both_m = int((final['Comparison_Status'] == 'Both Missed').sum())

    print("\n" + "=" * 55)
    print("📊 FINAL PERFORMANCE REPORT (FULL ANALYSIS)")
    print("=" * 55)
    print(f"Total Companies Analyzed:           {total_rows}")
    print("-" * 55)
    print(f"Total Companies found by SNOVIO:    {s_only + both_f}  ✅")
    print(f"Total Companies found by PREVIOUS:  {p_only + both_f}")
    print("-" * 55)
    print(f"1. Success only with Snovio:        {s_only}")
    print(f"2. Success only with Previous:      {p_only}")
    print(f"3. Success with BOTH:               {both_f}")
    print(f"4. BOTH FAILED (Intersection):      {both_m}  ⚠️")
    print("-" * 55)
    print("TOTAL EMAIL VOLUME:")
    print(f"  > SNOVIO: {total_snov_emails} total ({pers_snov_emails} personal) ✅")
    print(f"  > PREVIOUS: {total_prev_emails} total ({pers_prev_emails} personal)")
    print("-" * 55)
    print("QUALITY ANALYSIS (Generic vs Personal):")
    print(f"  > Snovio Personal leads:          {pers_snov_emails}")
    print(f"  > Previous Personal leads:        {pers_prev_emails}")
    print(f"  > Snovio Generic (Useless):       {gen_snov_emails}")
    print(f"  > Previous Generic (Useless):     {gen_prev_emails} ❌")
    print("-" * 55)
    print(f"Detailed report saved as: {FILE_OUTPUT}")
    print("=" * 55)

except Exception as e:
    print(f"❌ Error: {e}")