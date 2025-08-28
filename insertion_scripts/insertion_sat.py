import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import hashlib

# --- CONFIGURATION ---
docker_db_config = {
    'host': 'db',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'sylla',
    'password': 'sylla'
}

csv_folder = "/usr/src/daily_data"
processed_files_path = os.path.join(csv_folder, "processed_files_sat.txt")

# --- Connexion DB ---
def connect_db():
    return psycopg2.connect(**docker_db_config)

# --- Lecture CSV ---
def get_csv_files():
    all_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    processed_files = []
    if os.path.exists(processed_files_path):
        with open(processed_files_path, "r") as f:
            processed_files = f.read().splitlines()
    new_files = [f for f in all_files if f not in processed_files]
    return new_files, processed_files

# --- Normaliser colonnes ---
def normalize_columns(df):
    df.columns = [c.strip().lower() for c in df.columns]  # tout en minuscule
    return df

# --- Génération hashkey ---
def generate_hashkey(value):
    return hashlib.md5(str(value).encode()).hexdigest()

# --- Colonnes par table SAT (tout en minuscules) ---
SAT_CONFIG = {
    'sat_customer': ('customerid', ['firstname','lastname','phone','email','supportrepid']),
    'sat_employee': ('employeeid', ['firstname','lastname','birthdate','hiredate','phone','fax','email','reportsto']),
    'sat_invoice': ('invoiceid', ['invoicedate']),
    'sat_invoiceline': ('invoicelineid', ['quantity']),
    'sat_track': ('trackid', ['unitprice']),
    'sat_company': ('companyid', ['companyname']),
    'sat_location': ('locationid', ['address','city','state','country','postalcode']),
    'sat_title': ('titleid', ['title'])
}

# --- Récupérer colonnes réelles d’une table ---
def get_table_columns(conn, table_name):
    query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return [row[0] for row in cur.fetchall()]

# --- Préparer données SAT ---
def prepare_sat(df, table_name, bk_col, attr_cols):
    if bk_col not in df.columns:
        df[bk_col] = range(1, len(df)+1)  # générer ID si absent

    sat_df = pd.DataFrame()
    sat_df[bk_col] = df[bk_col]

    # hashkey à partir de la BK
    hash_col = table_name.replace("sat_", "") + "_hashkey"
    sat_df[hash_col] = sat_df[bk_col].apply(generate_hashkey)

    for col in attr_cols:
        sat_df[col] = df[col] if col in df.columns else None

    sat_df["loaddate"] = datetime.now()
    sat_df["source"] = "data_daily"

    return sat_df

# --- Insertion robuste ---
def insert_sat(conn, table_name, df):
    bk_col, attr_cols = SAT_CONFIG[table_name]
    sat_df = prepare_sat(df, table_name, bk_col, attr_cols)

    # Colonnes de la table réelle
    table_cols = get_table_columns(conn, table_name)

    # On garde uniquement l’intersection (pour éviter l’erreur UndefinedColumn)
    final_cols = [c for c in sat_df.columns if c in table_cols]

    if not final_cols:
        print(f"⚠️ Aucune colonne valide trouvée pour {table_name}, rien inséré.")
        return

    sat_df = sat_df[final_cols]

    tuples = [tuple(x) for x in sat_df.to_numpy()]
    cols_sql = ','.join(final_cols)
    query = f'INSERT INTO {table_name} ({cols_sql}) VALUES %s ON CONFLICT DO NOTHING'

    with conn.cursor() as cur:
        execute_values(cur, query, tuples)
        conn.commit()
    print(f"➡️ Insertion dans {table_name}: {len(sat_df)} lignes")

# --- Script principal ---
def main():
    conn = connect_db()
    new_files, processed_files = get_csv_files()

    if not new_files:
        print("ℹ️ Aucun nouveau fichier CSV à traiter.")
        conn.close()
        return

    for file in new_files:
        file_path = os.path.join(csv_folder, file)
        df = pd.read_csv(file_path)
        df = normalize_columns(df)

        for table_name in SAT_CONFIG.keys():
            insert_sat(conn, table_name, df)

        processed_files.append(file)
        with open(processed_files_path, "a") as f:
            f.write(file + "\n")

    conn.close()
    print("\n✅ Insertion SAT terminée pour tous les fichiers CSV nouveaux.")

if __name__ == "__main__":
    main()
