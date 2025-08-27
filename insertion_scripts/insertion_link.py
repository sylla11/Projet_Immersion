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

# --- GENERATION AUTOMATIQUE DES ID ---
def generate_missing_ids(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

# --- INSERTION LINK ---
def insert_link(conn, table_name, df, id_col, foreign_cols, appointement=False):
    # Génération des UUID pour les colonnes manquantes
    df = generate_missing_ids(df, foreign_cols)
    
    # Préparer le DataFrame à insérer
    df_to_insert = df[foreign_cols].copy()
    df_to_insert[id_col] = [str(uuid.uuid4()) for _ in range(len(df_to_insert))]

    if appointement:
        df_to_insert['AppointementDate'] = None
    
    df_to_insert['Source'] = 'datadaily'
    df_to_insert['LoadDate'] = datetime.now()
    
    # Préparer et exécuter l'insertion
    tuples = [tuple(x) for x in df_to_insert.to_numpy()]
    cols = ','.join(df_to_insert.columns)
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    
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
        
        insert_link(conn, 'link_customercompany', df, 'CustomerCompanyId', ['CustomerId', 'CompanyId'])
        insert_link(conn, 'link_customerlocation', df, 'CustomerLocationId', ['CustomerId', 'LocationId'])
        insert_link(conn, 'link_customerinvoice', df, 'CustomerInvoiceId', ['CustomerId', 'InvoiceId'])
        insert_link(conn, 'link_employeecustomer', df, 'EmployeeCustomerId', ['CustomerId', 'EmployeeId'])
        insert_link(conn, 'link_employeelocation', df, 'EmployeeLocationId', ['EmployeeId', 'LocationId'])
        insert_link(conn, 'link_employeeinvoice', df, 'EmployeeInvoiceId', ['EmployeeId', 'InvoiceId'])
        insert_link(conn, 'link_employeetitle', df, 'EmployeeTitleId', ['EmployeeId', 'TitleId'], appointement=True)
        insert_link(conn, 'link_invoiceinvoiceline', df, 'InvoiceInvoicelineId', ['InvoicelineId', 'InvoiceId'])
        insert_link(conn, 'link_invoicelinetrack', df, 'InvoicelineTrackId', ['InvoicelineId', 'TrackId'])

        # Ajouter fichier traité à l'historique
        processed_files.append(file)
        with open(processed_files_path, "a") as f:
            f.write(file + "\n")
    
    conn.close()
    print("✅ Insertion LINK terminée pour tous les fichiers CSV nouveaux.")

if __name__ == "__main__":
    main()

