import pandas as pd
import re

# 1. Cargamos los archivos de Excel (.xlsx)
# Nota: He puesto los nombres tal cual aparecen en tu error
try:
    old_df = pd.read_excel('piano economico (3).xlsx')
    new_df = pd.read_excel('piano economico (2).xlsx')
    print("Archivos cargados correctamente.")
except FileNotFoundError as e:
    print(f"Error: No se encontró el archivo. Asegúrate de que estén en la misma carpeta. {e}")
    exit()

# Función para extraer y limpiar emails de una celda
def get_clean_emails(text):
    if pd.isna(text): return set()
    # Separamos por comas, espacios o saltos de línea
    parts = re.split(r'[,\s\n]', str(text))
    return {p.strip().lower() for p in parts if '@' in p}

# 2. Recopilamos los emails que ya conoce tu jefe (v3)
emails_ya_enviados = set()
column_name = 'Email ufficio competenza PD'

if column_name in old_df.columns:
    for cell in old_df[column_name]:
        emails_ya_enviados.update(get_clean_emails(cell))
else:
    print(f"Error: No encuentro la columna '{column_name}' en el archivo viejo.")
    exit()

# 3. Identificamos duplicados en la lista nueva (v2)
def es_repetido(cell):
    emails_celda = get_clean_emails(cell)
    return any(email in emails_ya_enviados for email in emails_celda)

# 4. Filtramos
new_df['es_duplicado'] = new_df[column_name].apply(es_repetido)

v2_limpia = new_df[new_df['es_duplicado'] == False].drop(columns=['es_duplicado'])

# 5. Guardamos el resultado (puedes guardarlo como Excel o CSV)
v2_limpia.to_excel('v2_sin_repetidos.xlsx', index=False)

print(f"Limpieza completada.")
print(f"Emails antiguos detectados: {len(emails_ya_enviados)}")
print(f"Filas eliminadas por repetición: {len(new_df) - len(v2_limpia)}")
print(f"Archivo guardado como: v2_sin_repetidos.xlsx")