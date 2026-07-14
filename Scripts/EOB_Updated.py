import pandas as pd
from pandas import json_normalize
from pathlib import Path
from sqlalchemy import create_engine, text
from Credentials import engine_PROD_Test_pyodbc, engine_PROD_Test as engine
import os
import json
from concurrent.futures import ThreadPoolExecutor
import pyodbc

filepath = Path(r"C:\BCDA\data")






def getwatermarks():
    query = """
    SELECT TOP 1 date
    FROM BCDA_data.dbo.watermark
    ORDER BY id DESC
    """

    timestamp = pd.read_sql(query, engine).iloc[0,0]

    query = """
        SELECT date
        FROM BCDA_data.dbo.watermark
        ORDER BY ID DESC
        OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY
    """

    timestamp_2 = pd.read_sql(query, engine).iloc[0,0]

    return timestamp, timestamp_2

extract_to, extract_from = getwatermarks()


def truncate_stging_tables(table_name):
    print("TRUNCATING:", table_name)
    with engine.begin() as conn:
        conn.execute(text(f"""
            IF OBJECT_ID('BCDA_Staging.dbo.{table_name}', 'U') IS NOT NULL
            truncate table BCDA_Staging.dbo.{table_name}
        """))



def load_ndjson(filepath):
    batch = []
    batch_size = 5000
    
    conn = pyodbc.connect(engine_PROD_Test_pyodbc)
    cursor = conn.cursor()
    
    BCDA_file = filepath.name
    BCDA_extract_date = pd.Timestamp.today()
    
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            batch.append((line, BCDA_extract_date, BCDA_file,  extract_from, extract_to))
    
            if len(batch) >= batch_size:
                cursor.executemany(
                    "INSERT INTO staging_bcda_raw (json_line, bcda_extract_date, bcda_file,  extract_from, extract_to) VALUES (?, ?, ?, ?, ?)",
                    batch
                )
                conn.commit()
                batch.clear()
    
    if batch:
        cursor.executemany(
            "INSERT INTO staging_bcda_raw (json_line, bcda_extract_date, bcda_file,  extract_from, extract_to) VALUES (?, ?, ?, ?, ?)",
            batch
        )
        conn.commit()
    
    cursor.close()
    conn.close()


def main():
    truncate_stging_tables("staging_bcda_raw")
    
    files = filepath.glob('ExplanationOfBenefit_*.ndjson')

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(load_ndjson, f) for f in files]

        for f in futures:
            f.result()

    engine.dispose()

if __name__ == "__main__":
    main()
