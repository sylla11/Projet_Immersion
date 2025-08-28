import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# --- CONFIGURATION ---
docker_db_config = {
    'host': 'db',  
    'port': 5432,
    'database': 'mydatabase',
    'user': 'sylla',
    'password': 'sylla'
}

csv_folder = "/usr/src/daily_data"
processed_files_path = os.path.join(csv_folder, "processed_files_link.txt")

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

# --- NORMALISER LES NOMS DE COLONNES (insensible à la casse) ---
def normalize_columns(df):
    df.columns = [c.strip() for c in df.columns]  # enlever espaces
    df.columns = [c[0].upper() + c[1:] if c else c for c in df.columns]  # majuscule initiale
    return df

# --- GÉNÉRER UNE COLONNE D’ID SI ABSENTE ---
def ensure_column(df, col_name):
    col_norm = next((c for c in df.columns if c.lower() == col_name.lower()), None)
    if not col_norm:
        df[col_name] = range(1, len(df)+1)
        print(f"⚠️ Colonne {col_name} absente ou vide, génération d'IDs séquentiels.")
    else:
        df[col_norm] = df[col_norm].fillna(range(1, len(df)+1))
        if df[col_norm].isnull().any():
            print(f"⚠️ Colonne {col_name} contient des valeurs nulles, génération d'IDs séquentiels pour les manquants.")
    return df

# --- INSERTION LINK ---
def insert_link(conn, table_name, df, link_id_col, foreign_cols, appointment=False):
    df = normalize_columns(df)
    df = ensure_column(df, link_id_col)
    for col in foreign_cols:
        df = ensure_column(df, col)

    df['Source'] = 'datadaily'
    df['LoadDate'] = datetime.now()
    if appointment:
        df['AppointmentDate'] = datetime.now()

    # Colonnes exactes à insérer
    cols_to_insert = [link_id_col] + foreign_cols + (['AppointmentDate'] if appointment else []) + ['Source','LoadDate']
    df_to_insert = df[cols_to_insert].copy()

    tuples = [tuple(x) for x in df_to_insert.to_numpy()]
    cols_sql = ','.join([f'"{c}"' for c in df_to_insert.columns])
    query = f'INSERT INTO {table_name} ({cols_sql}) VALUES %s ON CONFLICT DO NOTHING'

    with conn.cursor() as cur:
        execute_values(cur, query, tuples)
        conn.commit()

    print(f"➡️ Insertion dans {table_name}: {len(df_to_insert)} lignes")

# --- SCRIPT PRINCIPAL ---
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

        # Insertion LINK tables
        insert_link(conn, 'link_customercompany', df, 'CustomerCompanyId', ['CustomerId','CompanyId'])
        insert_link(conn, 'link_customerlocation', df, 'CustomerLocationId', ['CustomerId','LocationId'])
        insert_link(conn, 'link_customerinvoice', df, 'CustomerInvoiceId', ['CustomerId','InvoiceId'])
        insert_link(conn, 'link_employeecustomer', df, 'EmployeeCustomerId', ['CustomerId','EmployeeId'])
        insert_link(conn, 'link_employeelocation', df, 'EmployeeLocationId', ['EmployeeId','LocationId'])
        insert_link(conn, 'link_employeeinvoice', df, 'EmployeeInvoiceId', ['EmployeeId','InvoiceId'])
        insert_link(conn, 'link_employeetitle', df, 'EmployeeTitleId', ['EmployeeId','TitleId'], appointment=True)
        insert_link(conn, 'link_invoiceinvoiceline', df, 'InvoiceInvoiceLineId', ['InvoiceLineId','InvoiceId'])
        insert_link(conn, 'link_invoicelinetrack', df, 'InvoiceLineTrackId', ['InvoiceLineId','TrackId'])

        # Marquer le fichier comme traité
        processed_files.append(file)
        with open(processed_files_path, "a") as f:
            f.write(file + "\n")

    conn.close()
    print("\n✅ Insertion LINK terminée pour tous les fichiers CSV nouveaux.")

if __name__ == "__main__":
    main()