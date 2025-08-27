import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import uuid

# --- CONFIGURATION ---
docker_db_config = {
    'host': 'my-postgres',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'sylla',
    'password': 'sylla'
}

csv_folder = "/usr/src/daily_data"
processed_files_path = os.path.join(csv_folder, "processed_files_hub.txt")

# --- CONNEXION A POSTGRES ---
def connect_db():
    return psycopg2.connect(**docker_db_config)

# --- LECTURE DES CSV ---
def get_csv_files():
    all_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    processed_files = []
    if os.path.exists(processed_files_path):
        with open(processed_files_path, "r") as f:
            processed_files = f.read().splitlines()
    new_files = [f for f in all_files if f not in processed_files]
    return new_files, processed_files

# --- GENERATION AUTOMATIQUE D'ID pour les 3 colonnes spécifiques ---
def ensure_specific_ids(df):
    for col, prefix in [('CompanyId','COMP'), ('LocationId','LOC'), ('TitleId','TITLE')]:
        if col not in df.columns:
            df[col] = [f"{prefix}_{uuid.uuid4().hex[:8]}" for _ in range(len(df))]
            print(f"⚠️ Colonne {col} manquante, génération d'IDs uniques.")
        else:
            df[col] = df[col].astype(str)
    return df

# --- INSERTION DANS LA BASE ---
def insert_hub(conn, table_name, df, id_col):
    if id_col not in df.columns:
        print(f"⚠️ Colonne {id_col} absente dans le CSV, on saute {table_name}")
        return
    df_to_insert = df.copy()
    df_to_insert['source'] = 'datadaily'
    df_to_insert['loadDate'] = datetime.now()
    
    tuples = [tuple(x) for x in df_to_insert.to_numpy()]
    cols = ','.join(df_to_insert.columns)
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING"
    
    with conn.cursor() as cur:
        execute_values(cur, query, tuples)
        conn.commit()

# --- SCRIPT PRINCIPAL ---
def main():
    conn = connect_db()
    new_files, processed_files = get_csv_files()
    
    for file in new_files:
        file_path = os.path.join(csv_folder, file)
        df = pd.read_csv(file_path)

        # Génération automatique uniquement pour CompanyId, LocationId, TitleId
        df = ensure_specific_ids(df)

        # Insertion dans les HUB tables
        insert_hub(conn, 'hub_customers', df, 'CustomerId')
        insert_hub(conn, 'hub_employees', df, 'EmployeeId')
        insert_hub(conn, 'hub_locations', df, 'LocationId')
        insert_hub(conn, 'hub_company', df, 'CompanyId')
        insert_hub(conn, 'hub_titles', df, 'TitleId')
        insert_hub(conn, 'hub_tracks', df, 'TrackId')
        insert_hub(conn, 'hub_invoices', df, 'InvoiceId')
        insert_hub(conn, 'hub_invoiceline', df, 'InvoiceLineId')

        # Ajouter fichier traité à l'historique
        processed_files.append(file)
        with open(processed_files_path, "a") as f:
            f.write(file + "\n")
    
    conn.close()
    print("✅ Insertion HUB terminée pour tous les fichiers CSV nouveaux.")

if __name__ == "__main__":
    main()
