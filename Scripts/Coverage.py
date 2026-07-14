import pandas as pd
from pandas import json_normalize
from pathlib import Path
from sqlalchemy import create_engine, text
from Credentials import engine_DEV_Final as engine
from datetime import datetime
import os
import json
from concurrent.futures import ThreadPoolExecutor


filepath = Path(r"C:\BCDA\Data")

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

def load_ndjson(file):
    records = []
    with open(file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"Skipping bad line in {filepath}")
    coverage_df = pd.json_normalize(records)
    coverage_df['beneficiary.reference']=coverage_df['beneficiary.reference'].str.split('/').str[1].copy()    
    return coverage_df

def get_reference_year(extensions):
    if not isinstance(extensions, list):
        return None
    
    for ext in extensions:
        if ext.get("url") == "https://bluebutton.cms.gov/resources/variables/rfrnc_yr":
            return ext.get("valueDate")
    return None

def coverage_basetable(coverage_df, file):
    coverage_df = coverage_df.explode('payor')
    coverage_df = coverage_df.explode('relationship.coding')
    coverage_df = coverage_df.explode('type.coding')
    coverage_df = json_normalize(
    coverage_df.to_dict(orient='records')
    )
    coverage_df = coverage_df.rename(
        columns={
            'payor.identifier.value': 'payor',
            'beneficiary.reference': 'Patient_ID',
            'relationship.coding.display':'relationship',
            'id': 'Coverage_ID',
            'period.start': 'period_start',
            'period.end': 'period_end',
            'meta.lastUpdated': 'meta_lastUpdated'
        }
    )
    coverage_df["reference_year"] = coverage_df["extension"].apply(get_reference_year)
    
    cols = ['Coverage_ID','Patient_ID','reference_year', 'subscriberId','status','period_start', 'period_end','payor', 
                                                'relationship','meta_lastUpdated','resourceType']
    coverage_df = coverage_df.reindex(columns = cols)
    coverage_df = coverage_df.drop_duplicates()
    coverage_df['bcda_file'] = file.name
    coverage_df['bcda_extract_date'] = pd.Timestamp.today()  
    coverage_df['extract_from'] = extract_from
    coverage_df['extract_to'] = extract_to     
    return coverage_df


def extension_table(coverage_df, file):
    ext_df = json_normalize(
    coverage_df.to_dict(orient='records'),
    record_path='extension',
    meta=['id', 'beneficiary.reference']
    )
    
    ext_df['Entitlement_Code'] = (
        ext_df['valueCoding.system']
        .astype(str)
        .str.rsplit('/', n=1)
        .str[-1]
    )
    
    ext_df = ext_df.rename(columns={
        'id': 'Coverage_ID',
        'valueDate': 'Year',
        'valueCoding.code': 'Medicare_Code',
        'valueCoding.display': 'Medicare_Display',
        'url': 'Medicare_Resource_url',
        'beneficiary.reference': 'Patient_ID'
    })
    
    extension_table = ext_df[['Coverage_ID', 'Patient_ID', 'Year', 'Medicare_Code', 'Medicare_Display','Entitlement_Code', 'Medicare_Resource_url']].copy()
    extension_table['bcda_file'] = file.name
    extension_table['bcda_extract_date'] = pd.Timestamp.today()  
    extension_table['extract_from'] = extract_from
    extension_table['extract_to'] = extract_to
    return extension_table


def load_to_sql(df, table_name, file_name, engine):
    df = df.copy()
    try:
        df.to_sql(
            name=table_name,
            con=engine,        # <- Engine, not conn or raw_conn
            if_exists='append',
            index=False,
            chunksize=1000
        )
    except Exception as e:
        print("SQL INSERT FAILED")
        print(e)
        raise




def process_file(file):
    print(f'Processing file: {file}')
    
    coverage_df = load_ndjson(file)

    basetable_df = coverage_basetable(coverage_df, file)
    extension_df = extension_table(coverage_df, file)
    
    load_to_sql(basetable_df, 'Coverage_Basetable_Staging', file.name, engine)
    load_to_sql(extension_df, 'Coverage_Extensiontable_Staging', file.name, engine)
    
    print(f'Loaded {file} to database')    



def main():
    truncate_stging_tables('Coverage_Basetable_Staging')
    truncate_stging_tables('Coverage_Extensiontable_Staging')

    files = filepath.glob('Coverage_*.ndjson')

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(process_file, files)

    engine.dispose()

if __name__ == "__main__":
    main()