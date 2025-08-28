import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import numpy as np

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

# --- GENERATION D'IDS NUMERIQUES ---
def generate_numeric_ids(df, id_col):
    """Génère des IDs numériques uniques pour une colonne manquante"""
    if id_col not in df.columns:
        numeric_ids = range(1000000, 1000000 + len(df))
        df[id_col] = numeric_ids
        print(f"✅ Colonne {id_col} générée numériquement")
    return df

# --- DEFINITION DES COLONNES PAR TABLE LINK ---
def get_link_table_columns(table_name):
    """Retourne les colonnes attendues pour chaque table LINK"""
    link_columns = {
        'link_customercompany': ['customercompanyid', 'customerid', 'companyid'],
        'link_customerlocation': ['customerlocationid', 'customerid', 'locationid'],
        'link_customerinvoice': ['customerinvoiceid', 'customerid', 'invoiceid'],
        'link_employeecustomer': ['employeecustomerid', 'employeeid', 'customerid'],
        'link_employeeinvoice': ['employeeinvoiceid', 'employeeid', 'invoiceid'],
        'link_employeelocation': ['employeelocationid', 'employeeid', 'locationid'],
        'link_employeetitle': ['employeetitleid', 'employeeid', 'titleid'],
        'link_invoiceinvoiceline': ['invoiceinvoicelineid', 'invoiceid', 'invoicelineid'],
        'link_invoicelinetrack': ['invoicelinetrackid', 'invoicelineid', 'trackid']
    }
    return link_columns.get(table_name.lower(), [])

# --- FILTRAGE DES COLONNES ---
def filter_columns_for_link_table(df, table_name):
    """Filtre le DataFrame pour ne garder que les colonnes de la table LINK"""
    expected_columns = get_link_table_columns(table_name)
    
    # Garder seulement les colonnes qui existent dans le DataFrame ET sont attendues
    available_columns = [col for col in expected_columns if col in df.columns]
    
    # Si aucune colonne attendue n'est disponible, retourner None
    if not available_columns:
        return None
    
    return df[available_columns].copy()

# --- INSERTION DANS LA BASE ---
def insert_link(conn, table_name, df, id_col):
    # Convertir toutes les colonnes en minuscules pour PostgreSQL
    df.columns = [c.lower() for c in df.columns]
    id_col = id_col.lower()

    # Générer les IDs manquants numériquement
    df = generate_numeric_ids(df, id_col)

    # Filtrer les colonnes pour ne garder que celles de la table LINK
    df_filtered = filter_columns_for_link_table(df, table_name)
    
    if df_filtered is None:
        print(f"⚠️ Aucune colonne valide pour {table_name}, insertion ignorée")
        return

    df_to_insert = df_filtered.copy()
    df_to_insert['source'] = 'datadaily'
    df_to_insert['loaddate'] = datetime.now()

    tuples = [tuple(x) for x in df_to_insert.to_numpy()]
    cols = ','.join(df_to_insert.columns)
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING"

    try:
        with conn.cursor() as cur:
            execute_values(cur, query, tuples)
            conn.commit()
        print(f"✅ Données insérées dans {table_name}")
    except Exception as e:
        print(f"❌ Erreur lors de l'insertion dans {table_name}: {e}")
        conn.rollback()

# --- SCRIPT PRINCIPAL ---
def main():
    conn = connect_db()
    new_files, processed_files = get_csv_files()

    if not new_files:
        print("✅ Aucun nouveau fichier CSV à traiter")
        conn.close()
        return

    for file in new_files:
        print(f"📂 Traitement du fichier: {file}")
        file_path = os.path.join(csv_folder, file)
        
        try:
            df = pd.read_csv(file_path)
            print(f"📊 Fichier chargé: {len(df)} lignes")
            
            # Insertion dans les LINK tables
            insert_link(conn, 'link_customercompany', df, 'CustomerCompanyId')
            insert_link(conn, 'link_customerlocation', df, 'CustomerLocationId')
            insert_link(conn, 'link_customerinvoice', df, 'CustomerInvoiceId')
            insert_link(conn, 'link_employeecustomer', df, 'EmployeeCustomerId')
            insert_link(conn, 'link_employeeinvoice', df, 'EmployeeInvoiceId')
            insert_link(conn, 'link_employeelocation', df, 'EmployeeLocationId')
            insert_link(conn, 'link_employeetitle', df, 'EmployeeTitleId')
            insert_link(conn, 'link_invoiceinvoiceline', df, 'InvoiceInvoiceLineId')
            insert_link(conn, 'link_invoicelinetrack', df, 'InvoiceLineTrackId')

            # Ajouter fichier traité à l'historique
            processed_files.append(file)
            with open(processed_files_path, "a") as f:
                f.write(file + "\n")
                
            print(f"✅ Fichier {file} traité avec succès\n")
            
        except Exception as e:
            print(f"❌ Erreur lors du traitement de {file}: {e}")
            continue

    conn.close()
    print("✅ Insertion LINK terminée pour tous les fichiers CSV nouveaux.")

if __name__ == "__main__":
    main()