import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import uuid

# --- CONFIGURATION ---
docker_db_config = {
    'host': 'my-postgres',      
    'port': 5432,               # port exposé du conteneur
    'database': 'mydatabase',
    'user': 'sylla',
    'password': 'sylla'
}

# Dossier contenant les CSV
csv_folder = "/usr/src/daily_data"

# Historique des fichiers déjà lus pour éviter les doublons
processed_files_path = os.path.join(csv_folder, "processed_files.txt")

# --- CONNEXION A POSTGRES ---
def connect_db():
    conn = psycopg2.connect(**docker_db_config)
    return conn

# --- LECTURE DES CSV ---
def get_csv_files():
    all_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    processed_files = []
    if os.path.exists(processed_files_path):
        with open(processed_files_path, "r") as f:
            processed_files = f.read().splitlines()
    new_files = [f for f in all_files if f not in processed_files]
    return new_files, processed_files

# --- RENOMMAGE DES COLONNES ---
def rename_columns(df):
    mapping = {
        "CustomerCompany": "CompanyName",
        "CustomerAddress": "Address",
        "CustomerCity": "City",
        "CustomerStat": "State",
        "CustomerCountry": "Country",
        "BillingAddress": "Address",
        "BillingCity": "City",
        "BillingCountry": "Country",
        "BillingPostalCode": "PostalCode",
        "BillingState": "State",
        "EmployeeTitle": "Title",
        "EmployeeAddress": "Address",
        "EmployeeCity": "City",
        "EmployeeState": "State",
        "EmployeeCountry": "Country"
    }
    df.rename(columns=mapping, inplace=True)
    return df

# --- INSERTION DANS LA BASE ---
def insert_data(conn, table_name, df, id_col_name, selected_columns):
    df_to_insert = df[selected_columns].copy()
    df_to_insert[id_col_name] = [str(uuid.uuid4()) for _ in range(len(df_to_insert))]
    df_to_insert['Source'] = 'datadaily'
    df_to_insert['LoadDate'] = datetime.now()
    
    # Créer la liste des tuples pour insertion
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
        df = rename_columns(df)
        
        # SAT_Customer
        insert_data(conn, 'SAT_Customer', df, 'SAT_CustomerId', [
            'CustomerPhone', 'CustomerEmail', 'CustomerSupportRepId',
            'CustomerId', 'CustomerFirstName', 'CustomerLastName'
        ])
        
        # SAT_Employee
        insert_data(conn, 'SAT_Employee', df, 'SAT_EmployeeId', [
            'EmployeeId', 'EmployeeFirstName', 'EmployeeLastName', 
            'EmployeeBirthDate', 'EmployeeHireDate', 'EmployeePhone',
            'EmployeeFax', 'EmployeeEmail', 'EmployeeReportsTo'
        ])
        
        # SAT_Location
        insert_data(conn, 'SAT_Location', df, 'SAT_LocationId', [
            'LocationId', 'Address', 'City', 'Country'
        ])
        
        # SAT_Invoiceline
        insert_data(conn, 'SAT_Invoiceline', df, 'SAT_InvoicelineId', [
            'InvoicelineId', 'Quantity'
        ])
        
        # SAT_Track
        insert_data(conn, 'SAT_Track', df, 'SAT_TrackId', [
            'TrackId', 'UnitPrice'
        ])
        
        # SAT_Invoice
        insert_data(conn, 'SAT_Invoice', df, 'SAT_InvoiceId', [
            'InvoiceId', 'InvoiceDate'
        ])
        
        # SAT_Company
        insert_data(conn, 'SAT_Company', df, 'SAT_CompanyId', [
            'CompanyId', 'CompanyName'
        ])
        
        # SAT_Title
        insert_data(conn, 'SAT_Title', df, 'SAT_TitleId', [
            'TitleId', 'Title'
        ])
        
        # Ajouter fichier traité à l'historique
        processed_files.append(file)
        with open(processed_files_path, "a") as f:
            f.write(file + "\n")
    
    conn.close()
    print("Traitement terminé pour tous les fichiers CSV nouveaux.")

if __name__ == "__main__":
    main()
