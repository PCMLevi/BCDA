import pandas as pd
from pandas import json_normalize
from pathlib import Path
from sqlalchemy import create_engine, text
from Credentials import engine_DEV_Final as engine
import os
import json
from concurrent.futures import ThreadPoolExecutor

data_dir = Path(r"C:\BCDA\data")

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

    records = []

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"Skipping bad line in {filepath}")
    patients_df = pd.json_normalize(records)
    return patients_df

    
def patient_basetable(patients_df, file):
    
    patients_address = json_normalize(
        patients_df.to_dict(orient='records'),
        record_path='address',
        meta=[col for col in patients_df.columns if col != 'address'],
        )

    patients_name = json_normalize(
        patients_address.to_dict(orient='records'),
        record_path='name',
        meta=[col for col in patients_address.columns if col != 'name'],
    )

    
    patients_name['deceasedBoolean'] = (
        patients_name['deceasedBoolean'].astype('boolean').fillna(False)
        | patients_name['deceasedDateTime'].notna()
    ).astype(bool)
        
    patients_df_extension = json_normalize(
    patients_df.to_dict(orient='records'),  
    record_path='extension',
    meta=['id'],
    errors = 'ignore'       
    )
    
    race = patients_df_extension[
        patients_df_extension['valueCoding.code']
        .astype(str)
        .str.match(r'^\d$')
    ].copy()
    patients_basetable_staging= patients_name.merge(race[['id', 'valueCoding.code', 'valueCoding.display']], left_on='id', right_on='id', how='left')
    patients_basetable_staging.rename(columns={'valueCoding.code': 'race_code', 'valueCoding.display': 'race', 'deceasedDateTime': 'deathdate', 'id': 'Patient_ID'}, inplace=True)

    patients_basetable_staging['bcda_extract_date'] = pd.Timestamp.today() 
    patients_basetable_staging['bcda_file'] = file.name
    patients_basetable_staging['extract_from'] = extract_from
    patients_basetable_staging['extract_to'] = extract_to
    patients_basetable_staging['line'] = (
        patients_basetable_staging['line']
        .apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    )

    patients_basetable_staging['given'] = (
        patients_basetable_staging['given']
        .apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    )
    
    patients_basetable=patients_basetable_staging[['Patient_ID', 'birthDate', 'gender', 'family', 'given', 'deceasedBoolean','deathdate', 'city', 'state', 'line', 'postalCode','race', 'race_code', 'bcda_extract_date', 'bcda_file', 'extract_from', 'extract_to']].copy()
    return patients_basetable

def patient_linktable(patients_df, file):
    
    patients_df_identifier = json_normalize(
    patients_df.to_dict(orient='records'),  
    record_path='identifier',
    meta=['id'],
    errors = 'ignore'
    )
    
    patients_identifier_dedup = patients_df_identifier[
        patients_df_identifier['value'].astype(str) != patients_df_identifier['id'].astype(str)
    ].copy()
    
    patients_identifier_dedup_type = json_normalize(
    patients_identifier_dedup.to_dict(orient='records'),  
    record_path='type.coding',
    meta=['id', 'value', 'period.start'],
    errors = 'ignore'
    )
    
    patients_identifier_dedup_type.rename(columns={'value': 'Bene_MBI', 'id': 'Patient_ID'}, inplace=True)
    patients_linktable = patients_identifier_dedup_type[
        ['Patient_ID', 'Bene_MBI', 'period.start']
    ].copy()
    patients_linktable['bcda_extract_date'] = pd.Timestamp.today()
    patients_linktable['bcda_file'] = file.name
    patients_linktable['extract_from'] = extract_from
    patients_linktable['extract_to'] = extract_to
    return patients_linktable


def patient_medicareenrollment(patients_df, file):
    
    patients_df_extension = json_normalize(
        patients_df.to_dict(orient='records'),
        record_path='extension',
        meta=['id'],
        errors='ignore'
    )

    valuedate = patients_df_extension[
        patients_df_extension['valueDate'].notna()
    ]

    medicare_enrollment = patients_df_extension[
        patients_df_extension['url']
        .astype(str)
        .str.match(r'^.*\d{2}$')
    ].copy()

    medicare_enrollment['url'] = medicare_enrollment['url'].str[-2:]

    medicare_enrollment = medicare_enrollment.drop(
        columns=['valueDate']
    ).copy()

    medicare_enrollment = medicare_enrollment.merge(
        valuedate[['id', 'valueDate']],
        on='id',
        how='left'
    ).copy()

    medicare_enrollment.rename(
        columns={
            'id': 'Patient_ID',
            'valueCoding.code': 'Medicare_Code',
            'valueCoding.display': 'Medicare_Display',
            'url': 'enrollment_month',
            'valueDate': 'enrollment_year'
        },
        inplace=True
    )

    medicare_enrollment['bcda_extract_date'] = pd.Timestamp.today()
    medicare_enrollment['bcda_file'] = file.name
    medicare_enrollment['extract_from'] = extract_from
    medicare_enrollment['extract_to'] = extract_to
    
    medicare_enrollment = medicare_enrollment[
        [
            'Patient_ID',
            'Medicare_Code',
            'Medicare_Display',
            'enrollment_month',
            'enrollment_year',
            'bcda_extract_date',
            'bcda_file',
            'extract_from',
            'extract_to'
        ]
    ].copy()

    return medicare_enrollment


def load_to_sql(df, table_name, file_name, engine):

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )


def process_file(file):
    print(f"Processing {file.name}")

    patients_df = load_ndjson(file)
    
    basetable_df = patient_basetable(patients_df, file)
    linktable_df = patient_linktable(patients_df, file)
    enrollment_df = patient_medicareenrollment(patients_df, file)

    load_to_sql(basetable_df, 'Patient_Basetable_Staging', file.name, engine)
    load_to_sql(linktable_df, 'Patient_Linktable_Staging', file.name, engine)
    load_to_sql(enrollment_df, 'Patient_MedicareEnrollment_Staging', file.name, engine)
    
    print(f'Loading {file} to database')


def main():
        
    truncate_stging_tables('Patient_Basetable_Staging')
    truncate_stging_tables('Patient_Linktable_Staging')
    truncate_stging_tables('Patient_MedicareEnrollment_Staging')    
    
    files = data_dir.glob("Patient_*.ndjson")

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(process_file, files)
    
    engine.dispose()


if __name__ == '__main__':
    main()


