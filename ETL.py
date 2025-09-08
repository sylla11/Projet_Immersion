#!/usr/bin/env python3
import os
import sys
import glob
import re
import pandas as pd
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

# -------------------------------
# Constantes
# -------------------------------
DATA_DIR = "/usr/src/daily_data"
LOG_FILE = LOG_FILE = "/usr/src/processed_files.txt"
   # ✅ chemin absolu pour persistance

# -------------------------------
# Gestion des fichiers déjà traités
# -------------------------------
def load_processed_files():
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE, "r") as f:
        return set(line.strip() for line in f)

def save_processed_file(filename):
    with open(LOG_FILE, "a") as f:
        f.write(filename + "\n")

# -------------------------------
# Recherche du CSV par date ou nom
# -------------------------------
def find_csv_file(date_str=None, filename=None):
    """
    Si date_str est fourni : recherche par date (YYYYMMDD)
    Si filename est fourni : recherche directe dans DATA_DIR
    """
    if filename:
        full_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(full_path):
            return full_path
        else:
            print(f"Erreur : le fichier {filename} n'existe pas dans {DATA_DIR}.")
            sys.exit(1)
    
    if date_str:
        pattern = os.path.join(DATA_DIR, f"*{date_str}*.csv")
        matching_files = glob.glob(pattern)
        if not matching_files:
            print(f"Aucun fichier trouvé pour la date {date_str}.")
            sys.exit(1)
        return matching_files[0]

    # Si aucun argument, prendre le dernier fichier CSV
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not files:
        print("Aucun fichier CSV trouvé dans le dossier.")
        sys.exit(1)
    files.sort(reverse=True)
    return files[0]

# -------------------------------
# Insertion DataFrame dans PostgreSQL
# -------------------------------
def insert_df(conn, df, table_name):
    if df.empty:
        print(f"⚠️  Aucun enregistrement à insérer pour {table_name}")
        return
    cur = conn.cursor()
    cols = ','.join([f'"{c}"' for c in df.columns])
    values = [tuple(x) for x in df.to_numpy()]
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING"
    try:
        execute_values(cur, query, values)
        conn.commit()
        print(f"✅ {len(df)} lignes insérées dans {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur lors de l'insertion dans {table_name} : {str(e)}")
    finally:
        cur.close()

# -------------------------------
# Traitement du CSV
# -------------------------------
def process_csv(file_path):
    print(f"Traitement du fichier : {file_path}")
    df = pd.read_csv(file_path, sep=",")
    df.columns = [c.strip().lower() for c in df.columns]
    print(df.columns)
    print(df.head(2))

    # Générer TitleId
    title_mapping = {title: i+1 for i, title in enumerate(df['employeetitle'].drop_duplicates())}
    df['titleid'] = df['employeetitle'].map(title_mapping).astype(int)

    # Générer LocationId
    df['locationkey'] = (
        df['customeraddress'].fillna('').astype(str) + '_' +
        df['customercity'].fillna('').astype(str) + '_' +
        df['customerstate'].fillna('').astype(str) + '_' +
        df['customercountry'].fillna('').astype(str) + '_' +
        df['billingpostalcode'].fillna('').astype(str)
    )
    location_mapping = {loc: i+1 for i, loc in enumerate(df['locationkey'].drop_duplicates())}
    df['locationid'] = df['locationkey'].map(location_mapping).astype(int)

    # Connexion PostgreSQL
    config = {
        'host': 'db',
        'port': 5432,
        'database': 'mydatabase',
        'user': 'sylla',
        'password': 'sylla'
    }
    conn = psycopg2.connect(**config)
    print("Connexion réussie !")

    now = datetime.now()

    # -------------------------------
    # HUBS
    # -------------------------------
    df_hub_customers = df[['customerid']].drop_duplicates()
    df_hub_customers['loaddate'] = now
    df_hub_customers['source'] = 'daily_data'

    df_hub_employees = df[['employeeid']].drop_duplicates()
    df_hub_employees['loaddate'] = now
    df_hub_employees['source'] = 'daily_data'

    df_hub_invoices = df[['invoiceid']].drop_duplicates()
    df_hub_invoices['loaddate'] = now
    df_hub_invoices['source'] = 'daily_data'

    df_hub_invoiceline = df[['invoicelineid']].drop_duplicates()
    df_hub_invoiceline['loaddate'] = now
    df_hub_invoiceline['source'] = 'daily_data'

    df_hub_tracks = df[['trackid']].drop_duplicates()
    df_hub_tracks['loaddate'] = now
    df_hub_tracks['source'] = 'daily_data'

    df_hub_titles = df[['titleid']].drop_duplicates()
    df_hub_titles['loaddate'] = now
    df_hub_titles['source'] = 'daily_data'

    df_hub_locations = df[['locationid']].drop_duplicates()
    df_hub_locations['loaddate'] = now
    df_hub_locations['source'] = 'daily_data'

    # -------------------------------
    # LINKS
    # -------------------------------
    df_link_customerinvoice = df[['customerid', 'invoiceid']].drop_duplicates()
    df_link_customerinvoice['loaddate'] = now
    df_link_customerinvoice['source'] = 'daily_data'

    df_link_customerlocation = df[['customerid', 'locationid']].drop_duplicates()
    df_link_customerlocation['loaddate'] = now
    df_link_customerlocation['source'] = 'daily_data'

    df_link_employeecustomer = df[['customerid', 'employeeid']].drop_duplicates()
    df_link_employeecustomer['loaddate'] = now
    df_link_employeecustomer['source'] = 'daily_data'

    df_link_employeeinvoice = df[['employeeid', 'invoiceid']].drop_duplicates()
    df_link_employeeinvoice['loaddate'] = now
    df_link_employeeinvoice['source'] = 'daily_data'

    df_link_employeelocation = df[['employeeid', 'locationid']].drop_duplicates()
    df_link_employeelocation['loaddate'] = now
    df_link_employeelocation['source'] = 'daily_data'

    df_link_employeetitle = df[['employeeid', 'titleid']].drop_duplicates()
    df_link_employeetitle['loaddate'] = now
    df_link_employeetitle['source'] = 'daily_data'
    df_link_employeetitle['appointmentdate'] = pd.to_datetime('today').normalize()

    df_link_invoiceinvoiceline = df[['invoicelineid', 'invoiceid']].drop_duplicates()
    df_link_invoiceinvoiceline['loaddate'] = now
    df_link_invoiceinvoiceline['source'] = 'daily_data'

    df_link_invoicelinetrack = df[['invoicelineid', 'trackid']].drop_duplicates()
    df_link_invoicelinetrack['loaddate'] = now
    df_link_invoicelinetrack['source'] = 'daily_data'

    # -------------------------------
    # SATELLITES
    # -------------------------------
    df_sat_customer = df[['customerid', 'customerfirstname', 'customerlastname', 'customerphone', 'customeremail']].drop_duplicates()
    df_sat_customer['loaddate'] = now
    df_sat_customer['source'] = 'daily_data'

    df_sat_location = df[['locationid',
                          'customeraddress', 'customercity', 'customerstate', 'customercountry', 'billingpostalcode',
                          'billingaddress', 'billingcity', 'billingstate', 'billingcountry',
                          'employeeaddress', 'employeecity', 'employeestate', 'employeecountry']].drop_duplicates()
    df_sat_location['loaddate'] = now
    df_sat_location['source'] = 'daily_data'

    df_sat_employee = df[['employeeid', 'employeefirstname', 'employeelastname', 
                          'employeebirthdate', 'employeehiredate', 'employeephone', 
                          'employeeemail', 'employeereportsto']].drop_duplicates()
    df_sat_employee['loaddate'] = now
    df_sat_employee['source'] = 'daily_data'

    df_sat_invoice = df[['invoiceid', 'invoicedate']].drop_duplicates()
    df_sat_invoice['loaddate'] = now
    df_sat_invoice['source'] = 'daily_data'

    df_sat_invoiceline = df[['invoicelineid', 'quantity']].drop_duplicates()
    df_sat_invoiceline['loaddate'] = now
    df_sat_invoiceline['source'] = 'daily_data'

    df_sat_track = df[['trackid', 'unitprice']].drop_duplicates()
    df_sat_track['loaddate'] = now
    df_sat_track['source'] = 'daily_data'

    df_sat_title = df[['titleid']].drop_duplicates()
    df_sat_title['loaddate'] = now
    df_sat_title['source'] = 'daily_data'

    # -------------------------------
    # Insertion dans PostgreSQL
    # -------------------------------
    hubs = [df_hub_customers, df_hub_employees, df_hub_invoices, df_hub_invoiceline, df_hub_tracks, df_hub_titles, df_hub_locations]
    links = [df_link_customerinvoice, df_link_customerlocation, df_link_employeecustomer, df_link_employeeinvoice, df_link_employeelocation, df_link_employeetitle, df_link_invoiceinvoiceline, df_link_invoicelinetrack]
    sats  = [df_sat_customer, df_sat_location, df_sat_employee, df_sat_invoice, df_sat_invoiceline, df_sat_track, df_sat_title]

    hub_names = ['hub_customers','hub_employees','hub_invoices','hub_invoiceline','hub_tracks','hub_titles','hub_locations']
    link_names = ['link_customerinvoice','link_customerlocation','link_employeecustomer','link_employeeinvoice','link_employeelocation','link_employeetitle','link_invoiceinvoiceline','link_invoicelinetrack']
    sat_names  = ['sat_customer','sat_location','sat_employee','sat_invoice','sat_invoiceline','sat_track','sat_title']

    for df_h, name in zip(hubs, hub_names):
        insert_df(conn, df_h, name)
    for df_l, name in zip(links, link_names):
        insert_df(conn, df_l, name)
    for df_s, name in zip(sats, sat_names):
        insert_df(conn, df_s, name)

    conn.close()
    print("✅ Données chargées dans PostgreSQL.")

# -------------------------------
# Fonction principale
# -------------------------------
def main():
    if len(sys.argv) != 2:
        print("Usage: python ETL.py <YYYYMMDD ou nom_fichier>")
        sys.exit(1)

    arg = sys.argv[1]
    if arg.isdigit() and len(arg) == 8:
        date_str = arg
        csv_file = find_csv_file(date_str=date_str)
    else:
        # extraire la date depuis le nom de fichier
        m = re.search(r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})', arg)
        if not m:
            print("Erreur : impossible d'extraire la date du nom de fichier.")
            sys.exit(1)
        date_str = ''.join(m.groups())
        filename = os.path.basename(arg)
        csv_file = find_csv_file(filename=filename)

    processed_files = load_processed_files()
    if csv_file in processed_files:
        print(f"⚠️ Le fichier {csv_file} a déjà été traité.")
        sys.exit(0)

    process_csv(csv_file)
    save_processed_file(csv_file)
    print(f"✅ Fichier {csv_file} traité et marqué comme terminé.")

# -------------------------------
# Lancement du script
# -------------------------------
if __name__ == "__main__":
    main()
