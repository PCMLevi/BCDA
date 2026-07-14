import pandas as pd
from pandas import json_normalize
from pathlib import Path
from sqlalchemy import create_engine, text
from Credentials import engine_DEV_Test
import os
import json
from multiprocessing import Pool, cpu_count


filepath = Path(r"C:\BCDA\data")


def getwatermarks():
    engine = engine_DEV_Test
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

extract_from, extract_to = getwatermarks()


def truncate_stging_tables(table_name):
    engine = engine_DEV_Test
    print("TRUNCATING:", table_name)
    with engine.begin() as conn:
        conn.execute(text(f"""
            IF OBJECT_ID('BCDA_Data.dbo.{table_name}', 'U') IS NOT NULL
            truncate table BCDA_Data.dbo.{table_name}
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
    eob_df = pd.json_normalize(records)
    eob_df.rename(columns = {
        'id': 'Claim_ID',
        'patient.reference': 'Patient_ID'
    }, inplace = True)
    eob_df.columns = eob_df.columns.str.replace('.', '_')
    eob_df['Patient_ID'] = eob_df['Patient_ID'].str.split('/').str[-1]
    return eob_df

def eob_basetable(eob_df, file):
    list_columns = [
    col for col in eob_df.columns
    if isinstance(eob_df[col].dropna().iloc[0], list)
    ]
    eob_basetable = eob_df.drop(columns= list_columns)
    eob_basetable['bcda_extract_date'] = pd.Timestamp.today()
    eob_basetable['bcda_file'] = file.name
    eob_basetable['extract_from'] = extract_from
    eob_basetable['extract_to'] = extract_to
    return eob_basetable


def eob_diagnosis(eob_df, file):
    eob_df_diag =json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='diagnosis',
        meta = 'Claim_ID'
    )

    eob_df_diag_diagnosis = eob_df_diag.explode('diagnosisCodeableConcept.coding')

    diagnosisCodeableConcept_coding = json_normalize(
        eob_df_diag_diagnosis.to_dict(orient='records'),
        meta='Claim_ID'
    )

    diagnosisCodeableConcept_type = diagnosisCodeableConcept_coding.explode('type')

    diagnosisCodeableConcept_type = json_normalize(
        diagnosisCodeableConcept_type.to_dict(orient='records'),
        meta='Claim_ID'
    )
    diagnosisCodeableConcept_type

    diagnosisCodeableConcept_type_2 = diagnosisCodeableConcept_type.explode('type.coding')

    diagnosisCodeableConcept_type = json_normalize(
        diagnosisCodeableConcept_type_2.to_dict(orient='records'),
        meta='Claim_ID'
    )

    #diagnosisCodeableConcept_type.drop(columns = ['diagnosisCodeableConcept.coding.system', 'type.coding.system', 'type.coding.code', 'extension'], inplace = True)
    diagnosisCodeableConcept_type.rename(columns = {
        'sequence': 'diagnosis_sequence',
        'diagnosisCodeableConcept.coding.code': 'diagnosis_code',
        'diagnosisCodeableConcept.coding.display': 'diagnosis_display',
        'type.coding.display': 'Diagnosis_Code_Type'
    }, inplace = True)
    diagnosisCodeableConcept_type = diagnosisCodeableConcept_type[['Claim_ID', 'diagnosis_sequence', 'diagnosis_code', 'diagnosis_display']]
    diagnosisCodeableConcept_type=diagnosisCodeableConcept_type.drop_duplicates()
# If it's a list, take the first element; otherwise leave as-is
    diagnosisCodeableConcept_type['diagnosis_sequence'] = diagnosisCodeableConcept_type['diagnosis_sequence'].apply(
        lambda x: x[0] if isinstance(x, list) else x
    )
    diagnosisCodeableConcept_type['bcda_extract_date'] = pd.Timestamp.today()
    diagnosisCodeableConcept_type['bcda_file'] = file.name
    diagnosisCodeableConcept_type['extract_from'] = extract_from
    diagnosisCodeableConcept_type['extract_to'] = extract_to
    return diagnosisCodeableConcept_type



def eob_benefitbalance(eob_df, file):
    eob_benefitbalance = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='benefitBalance',
        meta = 'Claim_ID'
    )

    eob_benefitbalance_category_coding = json_normalize(
        eob_benefitbalance.to_dict(orient='records'),
        record_path='category.coding',
        meta = ['Claim_ID', 'financial']
    )

    eob_benefitbalance_financial = json_normalize(
        eob_benefitbalance_category_coding.to_dict(orient='records'),
        record_path='financial',
        meta = [col for col in eob_benefitbalance_category_coding.columns if col != 'financial']
    )

    eob_benefitbalance_financial_type_coding = json_normalize(
        eob_benefitbalance_financial.to_dict(orient='records'),
        record_path='type.coding',
        meta = [col for col in eob_benefitbalance_financial.columns if col != 'type.coding'],
        record_prefix='type_coding_'
    )

    eob_benefitbalance_financial_type_coding.columns = eob_benefitbalance_financial_type_coding.columns.str.replace('.', '_')
    #eob_benefitbalance_financial_type_coding.drop(columns = ['system', 'type_coding_system', 'type_coding_code'], inplace = True)
    eob_benefitbalance_financial_type_coding.rename(columns = {
        'type_coding_display': 'type',
        'usedMoney_currency': 'currency_type',
        'usedMoney_value' : 'amount'
    }, inplace = True)
    
    if 'usedUnsignedInt' not in eob_benefitbalance_financial_type_coding.columns:
        eob_benefitbalance_financial_type_coding['usedUnsignedInt'] = None

    eob_benefitbalance_financial_type_coding = eob_benefitbalance_financial_type_coding[['Claim_ID', 'currency_type', 'amount', 'type', 'code', 'display', 'usedUnsignedInt']]
    eob_benefitbalance_financial_type_coding['bcda_extract_date'] = pd.Timestamp.today()
    eob_benefitbalance_financial_type_coding['bcda_file'] = file.name
    eob_benefitbalance_financial_type_coding['extract_from'] = extract_from
    eob_benefitbalance_financial_type_coding['extract_to'] = extract_to
    return eob_benefitbalance_financial_type_coding



def eob_careteam(eob_df, file):
    eob_careteam = json_normalize(
        eob_df.to_dict(orient= 'records'),
        record_path= 'careTeam',
        meta = 'Claim_ID'
    )

    eob_careteam_provider_coding = json_normalize(
        eob_careteam.to_dict(orient= 'records'),
        record_path= 'provider.identifier.type.coding',
        meta = [col for col in eob_careteam.columns if col != 'provider.identifier.type.coding']
    )

    eob_careteam_provider_coding_qual_0 = eob_careteam_provider_coding.explode('qualification.coding')

    eob_careteam_provider_coding_qual = json_normalize(
        eob_careteam_provider_coding_qual_0.to_dict(orient='records'),
        meta=[col for col in eob_careteam_provider_coding_qual_0.columns if col !='qualification.coding']
    )
    eob_careteam_provider_coding_qual =eob_careteam_provider_coding_qual.drop(columns='qualification.coding')
    eob_careteam_provider_coding_qual

    eob_careteam_provider_coding_qual_role_0 = eob_careteam_provider_coding_qual.explode('role.coding')
    eob_careteam_provider_coding_qual_role = json_normalize(
        eob_careteam_provider_coding_qual_role_0.to_dict(orient='records'),
        meta=[col for col in eob_careteam_provider_coding_qual_role_0.columns if col != 'role.coding']
    )

    eob_careteam_provider_coding_qual_role.columns = eob_careteam_provider_coding_qual_role.columns.str.replace('.', '_')
    eob_careteam_provider_coding_qual_role = eob_careteam_provider_coding_qual_role.drop(columns = 'extension')
    #eob_careteam_provider_coding_qual_role.drop(columns = ['system','code' , 'display', 'qualification_coding_system','role_coding_code','role_coding_system'], inplace = True)
    eob_careteam_provider_coding_qual_role.rename(columns = {
        'sequence': 'careteam_sequence',
        'qualification_coding_code': 'qualification_code',
        'qualification_coding_display': 'qualification_display',
        'role_coding_display': 'role'
    }, inplace = True)
    eob_careteam_provider_coding_qual_role = eob_careteam_provider_coding_qual_role[['Claim_ID', 'careteam_sequence', 'provider_identifier_value', 'qualification_code', 'qualification_display', 'role']]
    eob_careteam_provider_coding_qual_role['careteam_sequence'] = eob_careteam_provider_coding_qual_role['careteam_sequence'].apply(
        lambda x: x[0] if isinstance(x, list) else x
    )
    eob_careteam_provider_coding_qual_role['bcda_extract_date'] = pd.Timestamp.today()
    eob_careteam_provider_coding_qual_role['bcda_file'] = file.name
    eob_careteam_provider_coding_qual_role['extract_from'] = extract_from
    eob_careteam_provider_coding_qual_role['extract_to'] = extract_to
    return eob_careteam_provider_coding_qual_role
    

def eob_insurance(eob_df, file):  
    eob_df_insurance = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='insurance',
        meta = 'Claim_ID'
    )
    eob_df_insurance.drop(columns = 'coverage.extension', inplace = True)
    eob_df_insurance.rename(columns = {'coverage.reference': 'Coverage_ID'}, inplace = True)
    eob_df_insurance = eob_df_insurance[['Claim_ID', 'Coverage_ID', 'focal']]
    eob_df_insurance['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_insurance['bcda_file'] = file.name
    eob_df_insurance['extract_from'] = extract_from
    eob_df_insurance['extract_to'] = extract_to
    return eob_df_insurance
    


def eob_item(eob_df, file):
    eob_df_item = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta = 'Claim_ID'
    )

    eob_df_item2 = eob_df_item.drop(columns = ['adjudication', 'extension', 'locationCodeableConcept.extension', 'productOrService.extension', 'revenue.extension'])

    eob_df_item_category_coding = eob_df_item2.explode('category.coding')

    eob_df_item_category_coding = json_normalize(
        eob_df_item_category_coding.to_dict(orient='records'),
        meta = 'Claim_ID'
    )
    eob_df_item_category_coding = eob_df_item_category_coding.drop(columns = ['category.coding', 'category.coding.system'])

    eob_df_item_location_coding = eob_df_item_category_coding.explode('locationCodeableConcept.coding')

    eob_df_item_location_coding = json_normalize(
        eob_df_item_location_coding.to_dict(orient='records'),
        meta = 'Claim_ID'
    )

    eob_df_item_location_coding = eob_df_item_location_coding.drop(columns = ['locationCodeableConcept.coding.system', 'locationCodeableConcept.coding'])

    eob_df_item_product_coding = eob_df_item_location_coding.explode('productOrService.coding')

    eob_df_item_product_coding = json_normalize(
        eob_df_item_product_coding.to_dict(orient='records'),
        meta = 'Claim_ID'
    )

    eob_df_item_product_coding = eob_df_item_product_coding.drop(columns ='productOrService.coding.system')

    eob_df_item_revenue_coding = eob_df_item_product_coding.explode('revenue.coding')

    eob_df_item_revenue_coding = json_normalize(
        eob_df_item_revenue_coding.to_dict(orient='records'),
        meta = 'Claim_ID'
    )

    #eob_df_item_revenue_coding = eob_df_item_revenue_coding.drop(columns = ['revenue.coding.system', 'revenue.coding'])
    eob_df_item_revenue_coding.rename(columns = {
        'sequence': 'item_sequence',
        'quantity.value': 'quantity',
        'category.coding.code': 'category_code',
        'category.coding.display': 'category_display',
        'locationCodeableConcept.coding.code': 'location_code',
        'locationCodeableConcept.coding.display': 'location_display',
        'productOrService.coding.code': 'product_code',
        'productOrService.coding.display': 'product_display',
        'revenue.coding.code': 'revenue_code',
        'revenue.coding.display': 'revenue_display',
        'servicedPeriod.end': 'serviceperiod_end',
        'servicedPeriod.start': 'serviceperiod_start'
    }, inplace = True)
    eob_df_item_revenue_coding = eob_df_item_revenue_coding[[
        'Claim_ID',
        'item_sequence',
        'careTeamSequence',
        'diagnosisSequence',
        'product_code',
        'product_display',
        'revenue_code',
        'revenue_display',
        'category_code',
        'category_display',
        'location_code',
        'serviceperiod_start',
        'serviceperiod_end',
        'servicedDate',
        'location_display']].copy()
    eob_df_item_revenue_coding['careTeamSequence'] = eob_df_item_revenue_coding['careTeamSequence'].apply(lambda x: x[0] if isinstance(x, list) else x)
    eob_df_item_revenue_coding['diagnosisSequence'] = eob_df_item_revenue_coding['diagnosisSequence'].apply(lambda x: x[0] if isinstance(x, list) else x)
    eob_df_item_revenue_coding['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item_revenue_coding['bcda_file'] = file.name
    eob_df_item_revenue_coding['extract_from'] = extract_from
    eob_df_item_revenue_coding['extract_to'] = extract_to
    return eob_df_item_revenue_coding

def eob_item_adjudication(eob_df, file):
    eob_df_item = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta = 'Claim_ID'
    )

    eob_df_item_adjudication = json_normalize(
        eob_df_item.to_dict(orient='records'),
        record_path='adjudication',
        meta = 'Claim_ID'
    )

    eob_df_item_adjudication_reason = json_normalize(
        eob_df_item_adjudication.to_dict(orient='records'),
        record_path='category.coding',
        meta = [col for col in eob_df_item_adjudication.columns if col != 'category.coding']
    )

    eob_df_item_adjudication_reason_coding = eob_df_item_adjudication_reason.explode('reason.coding')

    eob_df_item_adjudication_reason_coding = json_normalize(
        eob_df_item_adjudication_reason_coding.to_dict(orient='records'),
        meta = 'Claim_ID',
    )

    eob_df_item_adjudication_reason_coding_ext = eob_df_item_adjudication_reason_coding.explode('extension')

    eob_df_item_adjudication_reason_coding_ext = json_normalize(
        eob_df_item_adjudication_reason_coding_ext.to_dict(orient='records'),
        meta = 'Claim_ID'
    )

    eob_df_item_adjudication_reason_coding_ext = eob_df_item_adjudication_reason_coding_ext[
        ~eob_df_item_adjudication_reason_coding_ext['code'].str.contains('http', na=False)
    ]

    #eob_df_item_adjudication_reason_coding_ext.drop(columns = [ 'code', 'system', 'extension', 'reason.coding.system', 'extension.url', 'extension.valueCoding.system', 'reason.coding'], inplace = True)
    eob_df_item_adjudication_reason_coding_ext.rename(columns = {
        'amount.currency': 'currency_type',
        'amount.value': 'amount',
        'reason.coding.code': 'reason_code',
        'reason.coding.display': 'reason_display',
        'extension.valueCoding.code': 'LINE_PMT_80_100_CD',
        'extension.valueCoding.display': 'LINE_PMT_80_100_display'
    }, inplace = True)
    eob_df_item_adjudication_reason_coding_ext = eob_df_item_adjudication_reason_coding_ext[['Claim_ID', 'display', 'currency_type', 'amount', 'reason_code', 'reason_display', 'LINE_PMT_80_100_CD', 'LINE_PMT_80_100_display']]

    eob_df_item_adjudication_reason_coding_ext_2=eob_df_item_adjudication_reason_coding_ext.reset_index(drop = True)
    eob_df_item_adjudication_reason_coding_ext_2['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item_adjudication_reason_coding_ext_2['bcda_file'] = file.name
    eob_df_item_adjudication_reason_coding_ext_2['extract_from'] = extract_from
    eob_df_item_adjudication_reason_coding_ext_2['extract_to'] = extract_to
    return eob_df_item_adjudication_reason_coding_ext_2

def eob_item_ext(eob_df, file):
    eob_df_item_ext = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta='Claim_ID'
    )
    eob_df_item_ext = json_normalize(
        eob_df_item_ext.to_dict(orient='records'),
        record_path='extension',
        meta = ['Claim_ID', 'locationCodeableConcept.extension', 'productOrService.extension', 'revenue.extension']
    )
    eob_df_item_ext = json_normalize(
        eob_df_item_ext.to_dict(orient='records'),
        record_path='locationCodeableConcept.extension',
        meta = [col for col in eob_df_item_ext.columns if col != 'locationCodeableConcept.extension'],
        record_prefix= 'loc_'
    )

    eob_df_item_ext.columns = eob_df_item_ext.columns.str.replace('.', '_')
    eob_df_item_ext = eob_df_item_ext[['Claim_ID'] + [c for c in eob_df_item_ext.columns if c != 'Claim_ID']]
    eob_df_item_ext.drop(columns= ['revenue_extension', 'productOrService_extension'], inplace = True)
    eob_df_item_ext['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item_ext['bcda_file'] = file.name
    eob_df_item_ext['extract_from'] = extract_from
    eob_df_item_ext['extract_to'] = extract_to
    return eob_df_item_ext





def eob_type(eob_df, file):
    eob_df_type = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='type_coding',
        meta='Claim_ID'
    )
    eob_df_type.rename(columns = {
        'code': 'clm_type_cd',
    }, inplace= True)
    eob_df_type = eob_df_type[['Claim_ID', 'clm_type_cd', 'display', 'system']]
    eob_df_type = eob_df_type[eob_df_type['system']=='https://bluebutton.cms.gov/resources/variables/nch_clm_type_cd']
    eob_df_type = eob_df_type.reset_index(drop = True)
    eob_df_type['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_type['bcda_file'] = file.name
    eob_df_type['extract_from'] = extract_from
    eob_df_type['extract_to'] = extract_to
    return eob_df_type


def eob_total(eob_df, file):
    eob_total = json_normalize(
    eob_df.to_dict(orient='records'),
    record_path='total',
    meta='Claim_ID'
    )
    eob_total

    eob_total_category = eob_total.explode('category.coding')

    eob_total_category = json_normalize(
        eob_total_category.to_dict(orient='records')
        , meta = 'Claim_ID'
    )
    #eob_total_category.drop(columns = ['category.coding.system', 'category.coding.code'], inplace = True)
    eob_total_category.rename(columns= {
        'amount.currency': 'currency_type',
        'amount.value': 'amount',
        'category.coding.display': 'category'
    }, inplace = True)
    eob_total_category = eob_total_category[['Claim_ID', 'category', 'currency_type', 'amount']]
    eob_total_category['bcda_extract_date'] = pd.Timestamp.today()
    eob_total_category['bcda_file'] = file.name
    eob_total_category['extract_from'] = extract_from
    eob_total_category['extract_to'] = extract_to
    return eob_total_category

def eob_contained(eob_df, file):
    eob_df_contained = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='contained',
        meta = 'Claim_ID'
    )
    eob_df_contained_identifier = json_normalize(
        eob_df_contained.to_dict(orient= 'records'),
        record_path='identifier',
        meta = [col for col in eob_df_contained.columns if col != 'identifier']
    )
    keep_columns = ['Claim_ID', 'NPI', 'active', 'name', 'resourceType']
    eob_df_contained_identifier = eob_df_contained_identifier[eob_df_contained_identifier['system'] == 'http://hl7.org/fhir/sid/us-npi']
    eob_df_contained_identifier = eob_df_contained_identifier.reset_index(drop = True)
    eob_df_contained_identifier.rename(columns = {'value':'NPI'}, inplace = True)
    #eob_df_contained_identifier.drop(columns=['status', 'code.coding', 'valueQuantity.value','meta.profile', 'type.coding', 'system', 'id'], inplace = True)
    eob_df_contained_identifier = eob_df_contained_identifier[['Claim_ID', 'NPI', 'active', 'name', 'resourceType']].copy()
    eob_df_contained_identifier['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_contained_identifier['bcda_file'] = file.name
    eob_df_contained_identifier['extract_from'] = extract_from
    eob_df_contained_identifier['extract_to'] = extract_to
    return eob_df_contained_identifier


def eob_procedure(eob_df, file):
    eob_df_procedure = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='procedure',
        meta = 'Claim_ID',
        errors='ignore'
    )
    
    eob_df_procedure_2 = json_normalize(
        eob_df_procedure.to_dict(orient='records'),
        record_path= 'procedureCodeableConcept.coding',
        meta = [col for col in eob_df_procedure.columns if col != 'procedureCodeableConcept.coding']
    )
    eob_df_procedure_2['display'] = eob_df_procedure_2['display'].str.replace('"', '')
    eob_df_procedure_2 = eob_df_procedure_2[['Claim_ID', 'sequence', 'code', 'display', 'system']].copy()
    eob_df_procedure_2['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_procedure_2['bcda_file'] = file.name
    eob_df_procedure_2['extract_from'] = extract_from
    eob_df_procedure_2['extract_to'] = extract_to
    return eob_df_procedure_2



def load_to_sql(df, table_name, file_name, engine):

    df = df.copy()

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=10000
        )

    except Exception as e:
        print(f"SQL INSERT FAILED {file_name}")
        print(e)
        raise

def process_file(file):
    
    engine = engine_DEV_Test
    
    print(f'Processing file: {file}')
    
    eob_df = load_ndjson(file)

    basetable_df = eob_basetable(eob_df, file)
    print(f'basetable done {file}')
    diagnosis_df = eob_diagnosis(eob_df, file)
    print(f'diagnosis done {file}')
    BenefitBalance_df = eob_benefitbalance(eob_df, file)
    print(f'benefit balance done {file}')
    careteam_df = eob_careteam(eob_df, file)
    print(f'careteam done {file}')
    insurance_df = eob_insurance(eob_df, file)
    print(f'insurance done {file}')
    item_df = eob_item(eob_df, file)
    print(f'item done {file}')
    item_adj_df = eob_item_adjudication(eob_df, file)
    print(f'item_adj done {file}')
    item_ext_df = eob_item_ext(eob_df, file)
    print(f'item_ext done {file}')
    type_df = eob_type(eob_df, file)
    print(f'type done {file}')
    total_df = eob_total(eob_df, file)
    print(f'total done {file}')
    contained_df = eob_contained(eob_df, file)
    print(f'contained done {file}')
    
    load_to_sql(basetable_df, 'EOB_Basetable_Staging', file.name, engine)
    print(f'EOB_Basetable_Staging loaded {file}')
    load_to_sql(diagnosis_df, 'EOB_Diagnosis_Staging', file.name, engine)
    print(f'EOB_Diagnosis_Staging loaded {file}')
    load_to_sql(BenefitBalance_df,'EOB_BenefitBalance_Staging', file.name, engine)
    print(f'EOB_BenefitBalance_Staging loaded {file}')
    load_to_sql(careteam_df,'EOB_careteam_Staging', file.name, engine)
    print(f'EOB_careteam_Staging loaded {file}')
    load_to_sql(insurance_df,'EOB_insurance_Staging', file.name, engine)
    print(f'EOB_insurance_Staging loaded {file}')
    load_to_sql(item_df, 'EOB_item_Staging', file.name, engine)
    print(f'EOB_item_Staging loaded {file}')
    load_to_sql(item_adj_df, 'EOB_item_adj_Staging', file.name, engine)
    print(f'EOB_item_adj_Staging loaded {file}')
    load_to_sql(item_ext_df, 'EOB_item_ext_Staging', file.name, engine)
    print(f'EOB_item_ext_Staging loaded {file}')
    load_to_sql(type_df, 'EOB_type_Staging', file.name, engine)
    print(f'EOB_type_Staging loaded {file}')
    load_to_sql(total_df, 'EOB_total_Staging', file.name, engine)
    print(f'EOB_total_Staging loaded {file}')
    load_to_sql(contained_df,'EOB_contained_Staging', file.name, engine)
    print(f'EOB_contained_Staging loaded {file}')
    
    print(f'Loaded {file} to database')    
    


def main():

    tables = [
        "EOB_Basetable_Staging",
        "EOB_Diagnosis_Staging",
        "EOB_BenefitBalance_Staging",
        "EOB_careteam_Staging",
        "EOB_insurance_Staging",
        "EOB_item_Staging",
        "EOB_item_ext_Staging",
        "EOB_item_adj_Staging",
        "EOB_type_Staging",
        "EOB_total_Staging",
        "EOB_contained_Staging"
        
    ]

    for t in tables:
        truncate_stging_tables(t)

    files = list(filepath.glob("ExplanationOfBenefit_*.ndjson"))

    workers = min(4, len(files))

    with Pool(workers) as pool:
        pool.map(process_file, files)

if __name__ == "__main__":
    main()
