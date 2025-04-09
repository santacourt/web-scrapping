import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import os
from io import StringIO


###

def setup_logging():
    log_file = os.path.join(os.path.dirname(__file__), 'web_scraping.log')
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def extract_table_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

 
    tables = soup.find_all('table')

    print(f"Tablas encontradas: {len(tables)}")

    if len(tables) < 1: 
        raise ValueError("No se encontró la tabla en la posición esperada.")

    
    correct_table = tables[0]


    df = pd.read_html(StringIO(str(correct_table)))[0]

    print("Tabla extraída exitosamente:")
    print(df.head())  
    print("Columnas del DataFrame:", df.columns)  
    return df

def transform_data(df, exchange_rates_csv):
    logging.info("Transformando %d filas de datos con tasas de cambio de: %s", len(df), exchange_rates_csv)
    rates = pd.read_csv(exchange_rates_csv, index_col=0).to_dict()['Rate']
    

    if len(df.columns) <= 2:
        print("Columnas disponibles en el DataFrame:", df.columns)
        raise KeyError("No hay suficientes columnas en el DataFrame.")
    

    market_cap_column = df.columns[2]
    
    df['MC_GBP_Billones'] = (df[market_cap_column] * rates['GBP']).round(2)
    df['MC_EUR_Billones'] = (df[market_cap_column] * rates['EUR']).round(2)
    df['MC_INR_Billones'] = (df[market_cap_column] * rates['INR']).round(2)
    logging.info("Datos transformados exitosamente")
    return df

def save_to_csv(df, output_csv):
    logging.info("Guardando datos en archivo CSV: %s", output_csv)
    df.to_csv(output_csv, index=False)
    logging.info("Archivo CSV guardado exitosamente")

def save_to_sqlite(df, db_name, table_name):
    logging.info("Guardando datos en base de datos SQLite: %s, tabla: %s", db_name, table_name)
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    logging.info("Datos guardados en la base de datos exitosamente")

def run_query(db_name, query):
    logging.info("Ejecutando consulta: %s", query)
    conn = sqlite3.connect(db_name)
    result = pd.read_sql_query(query, conn)
    conn.close()
    logging.info("Consulta ejecutada exitosamente")
    return result

def load_to_csv(df, output_csv):
    logging.info("Guardando DataFrame transformado en archivo CSV: %s", output_csv)
    df.to_csv(output_csv, index=False)
    logging.info("Archivo CSV guardado exitosamente")

def visualize_database(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
  
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tablas en la base de datos:")
    for table in tables:
        print(table[0])
        
        cursor.execute(f"SELECT * FROM {table[0]};")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    
    conn.close()


if __name__ == "__main__":
    setup_logging()
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    exchange_rates_csv = r"rute"
    output_csv = r"rute"
    db_name = r"rute"
    table_name = "largest_banks"

    df = extract_table_data(url)
    print("Columnas del DataFrame antes de la transformación:", df.columns)  
    df_transformed = transform_data(df, exchange_rates_csv)
    save_to_csv(df_transformed, output_csv)
    save_to_sqlite(df_transformed, db_name, table_name)
    load_to_csv(df_transformed, output_csv)

    queries = {
        "Londres": "SELECT `Bank name`, MC_GBP_Billones FROM largest_banks WHERE `Bank name` LIKE '%London%'",
        "Berlín": "SELECT `Bank name`, MC_EUR_Billones FROM largest_banks WHERE `Bank name` LIKE '%Berlin%'",
        "Nueva Delhi": "SELECT `Bank name`, MC_INR_Billones FROM largest_banks WHERE `Bank name` LIKE '%New Delhi%'"
    }

    for city, query in queries.items():
        result = run_query(db_name, query)
        print(f"Resultados para {city}:\n", result)