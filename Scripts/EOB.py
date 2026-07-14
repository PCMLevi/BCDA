import pandas as pd
from pandas import json_normalize
from pathlib import Path
from sqlalchemy import create_engine, text
from Credentials import engine_DEV_Final as engine
import os
import json
from concurrent.futures import ThreadPoolExecutor


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
    eob_basetable = eob_df.explode('insurance')
    eob_basetable = json_normalize(
        eob_basetable.to_dict(orient='records')
    )
    if 'facility_extension' in eob_basetable:
        eob_basetable = eob_basetable.explode('facility_extension')
        eob_basetable = json_normalize(
            eob_basetable.to_dict(orient='records')
        )
        eob_basetable = eob_basetable.rename(columns={
        'facility_extension.valueCoding.code': 'facility_Code_ext',
        'facility_extension.valueCoding.display': 'facility_Display'
        })
    if 'billablePeriod_extension' in eob_df.columns:
        eob_basetable = eob_basetable.explode('billablePeriod_extension')
        eob_basetable = json_normalize(
            eob_basetable.to_dict(orient='records')
        )
        eob_basetable = eob_basetable.rename(columns={
            'billablePeriod_extension.valueCoding.code': 'adjustment_code',
            'billablePeriod_extension.valueCoding.display': 'adjustment_type'
        })

    eob_basetable['insurance.coverage.reference'] = eob_basetable['insurance.coverage.reference'].str.split('/').str[-1]
    eob_basetable = eob_basetable.rename(columns={
        'id': 'Claim_ID',
        'insurance.coverage.reference' : 'insurance'
        })


    eob_basetable.columns = eob_basetable.columns.str.replace('.', '_')
    base_columns = [
        'Claim_ID', 
        'Patient_ID',
        'disposition',
        'outcome',
        'status',
        'billablePeriod_start',
        'billablePeriod_end',
        'adjustment_type',
        'adjustment_code',
        'payment_amount_currency',
        'payment_amount_value',
        'payment_date',
        'provider_identifier_value',
        'provider_display',
        'referral_display',
        'referral_identifier_value',
        'facility_identifier_value',
        'facility_Code_ext',
        'facility_Display',
        'insurance',
        'subType_text',
        'meta_lastUpdated',
        'created'
    ]
    eob_basetable = eob_basetable.reindex(columns = base_columns)
    eob_basetable['bcda_extract_date'] = pd.Timestamp.today()
    eob_basetable['bcda_file'] = file.name
    eob_basetable['extract_from'] = extract_from
    eob_basetable['extract_to'] = extract_to

    return eob_basetable


def eob_diagnosis(eob_df, file):
    if 'diagnosis' in eob_df.columns:
        eob_df_diag =json_normalize(
            eob_df.to_dict(orient='records'),
            record_path='diagnosis',
            meta = 'Claim_ID'
        )
        eob_df_diag_diagnosis = eob_df_diag.explode('diagnosisCodeableConcept.coding')
        diagnosisCodeableConcept_type = eob_df_diag_diagnosis.explode('type')
        if 'extension' in diagnosisCodeableConcept_type.columns:
            diagnosisCodeableConcept_type = diagnosisCodeableConcept_type.explode('extension')
        diagnosisCodeableConcept_type = json_normalize(
            diagnosisCodeableConcept_type.to_dict(orient='records')
        )
        diagnosisCodeableConcept_type_2 = diagnosisCodeableConcept_type.explode('type.coding')

        diagnosisCodeableConcept_type = json_normalize(
            diagnosisCodeableConcept_type_2.to_dict(orient='records')
        )
        diagnosisCodeableConcept_type.rename(columns = {
            'sequence': 'diagnosis_sequence',
            'diagnosisCodeableConcept.coding.code': 'diagnosis_code',
            'diagnosisCodeableConcept.coding.display': 'diagnosis_display',
            'type.coding.display': 'Diagnosis_Code_Type',
            'extension.valueCoding.code': 'ext_code',
            'extension.valueCoding.display':'ext_display'
        }, inplace = True)

        cols = ['Claim_ID', 'diagnosis_sequence', 'diagnosis_code', 'diagnosis_display', 'Diagnosis_Code_Type', 'ext_code', 'ext_display', 'created']

        diagnosisCodeableConcept_type = diagnosisCodeableConcept_type.reindex(columns= cols)
        if 'diagnosis_display' in diagnosisCodeableConcept_type.columns:
            diagnosisCodeableConcept_type['diagnosis_display'] = diagnosisCodeableConcept_type['diagnosis_display'].str.replace('"', '', regex=False)
        diagnosisCodeableConcept_type = diagnosisCodeableConcept_type.drop_duplicates().reset_index(drop = True)

        diagnosisCodeableConcept_type['bcda_extract_date'] = pd.Timestamp.today()
        diagnosisCodeableConcept_type['bcda_file'] = file.name
        diagnosisCodeableConcept_type['extract_from'] = extract_from
        diagnosisCodeableConcept_type['extract_to'] = extract_to
    else:
        diagnosisCodeableConcept_type= pd.DataFrame(columns = [
            'Claim_ID',
            'diagnosis_sequence',
            'diagnosis_code',
            'diagnosis_display',
            'Diagnosis_Code_Type',
            'ext_code',
            'ext_display',
            'created',
            'bcda_extract_date',
            'bcda_file',
            'extract_from',
            'extract_to'
        ])
    return diagnosisCodeableConcept_type



def eob_benefitbalance(eob_df, file):
    if 'benefitBalance' in eob_df.columns:
        eob_benefitbalance = json_normalize(
            eob_df.to_dict(orient='records'),
            record_path='benefitBalance',
            meta = ['Claim_ID', 'created']
        )
        if 'category.coding' in eob_benefitbalance.columns:
            eob_benefitbalance = eob_benefitbalance.explode('category.coding')
            eob_benefitbalance = json_normalize(
            eob_benefitbalance.to_dict(orient='records')
            )
            eob_benefitbalance.rename(columns = {
                'category.coding.code': 'code',
                'category.coding.display': 'display'
            }, inplace = True)
            #eob_benefitbalance_category_coding = json_normalize(
            #    eob_benefitbalance.to_dict(orient='records'),
            #    record_path='category.coding',
            #    meta = ['Claim_ID', 'financial']
            #)
        if 'financial' in eob_benefitbalance.columns:
            eob_benefitbalance = eob_benefitbalance.explode('financial')
            eob_benefitbalance = json_normalize(
            eob_benefitbalance.to_dict(orient='records')
            )
            eob_benefitbalance.rename(columns = {
            'financial.usedMoney.currency': 'currency_type',
            'financial.usedMoney.value' : 'amount'
            }, inplace = True)
        
            
            #eob_benefitbalance_financial = json_normalize(
            #    eob_benefitbalance_category_coding.to_dict(orient='records'),
            #    record_path='financial',
            #    meta = [col for col in eob_benefitbalance_category_coding.columns if col != 'financial']
            #)
        eob_benefitbalance = json_normalize(
            eob_benefitbalance.to_dict(orient='records')
        )
        if 'financial.type.coding' in eob_benefitbalance.columns:
            eob_benefitbalance = eob_benefitbalance.explode('financial.type.coding')
            eob_benefitbalance = json_normalize(
            eob_benefitbalance.to_dict(orient='records')
            )
            eob_benefitbalance.rename(columns = {
            'financial.type.coding.display': 'type',
            }, inplace = True)
        
        #eob_benefitbalance_financial_type_coding = json_normalize(
        #    eob_benefitbalance_financial.to_dict(orient='records'),
        #    record_path='type.coding',
        #    meta = [col for col in eob_benefitbalance_financial.columns if col != 'type.coding'],
        #    record_prefix='type_coding_'
        #)

        
        

        #if 'financial.usedUnsignedInt' not in eob_benefitbalance.columns:
        #    eob_benefitbalance['usedUnsignedInt'] = None
        if 'financial.usedUnsignedInt' in eob_benefitbalance.columns:
            eob_benefitbalance.rename(columns = {'financial.usedUnsignedInt':'usedUnsignedInt'}, inplace = True)
        
        cols = ['Claim_ID', 'currency_type', 'amount', 'type', 'code', 'display', 'usedUnsignedInt', 'created']
        eob_benefitbalance = eob_benefitbalance.reindex(columns = cols)
        #eob_benefitbalance_financial_type_coding[['Claim_ID', 'currency_type', 'amount', 'type', 'code', 'display', 'usedUnsignedInt']]
        eob_benefitbalance['bcda_extract_date'] = pd.Timestamp.today()
        eob_benefitbalance['bcda_file'] = file.name
        eob_benefitbalance['extract_from'] = extract_from
        eob_benefitbalance['extract_to'] = extract_to
    else:
        eob_benefitbalance= pd.DataFrame(columns = [
            'Claim_ID',
            'currency_type',
            'amount',
            'type',
            'code',
            'display',
            'usedUnsignedInt',
            'created',
            'bcda_extract_date',
            'bcda_file',
            'extract_from',
            'extract_to'
        ])
    return eob_benefitbalance



def eob_careteam(eob_df, file):
    eob_careteam = json_normalize(
        eob_df.to_dict(orient= 'records'),
        record_path= 'careTeam',
        meta = ['Claim_ID', 'created']
    )
    eob_careteam=eob_careteam.explode('provider.identifier.type.coding')
    eob_careteam=eob_careteam.explode('role.coding')
    eob_careteam=eob_careteam.explode('qualification.coding')
    eob_careteam = json_normalize(
        eob_careteam.to_dict(orient='records')
    )
    eob_careteam.columns = eob_careteam.columns.str.replace('.', '_')
    eob_careteam.rename(columns = {
        'sequence': 'careteam_sequence',
        'qualification_coding_code': 'qualification_code',
        'qualification_coding_display': 'qualification_display',
        'role_coding_display': 'role'
    }, inplace = True)
    eob_careteam = eob_careteam[['Claim_ID', 'careteam_sequence', 'provider_identifier_value', 'qualification_code', 'qualification_display', 'role', 'created']]
    eob_careteam['careteam_sequence'] = eob_careteam['careteam_sequence'].apply(
        lambda x: x[0] if isinstance(x, list) else x
    )
    eob_careteam['bcda_extract_date'] = pd.Timestamp.today()
    eob_careteam['bcda_file'] = file.name
    eob_careteam['extract_from'] = extract_from
    eob_careteam['extract_to'] = extract_to
    return eob_careteam




def eob_item(eob_df, file):
    eob_df_item = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta = ['Claim_ID', 'created']
    )
    if 'category.coding' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('category.coding')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        
        eob_df_item.rename(columns = {
        'category.coding.code': 'category_code',
        'category.coding.display': 'category_display'
        }, inplace = True) 

    if 'locationCodeableConcept.coding' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('locationCodeableConcept.coding')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        
        eob_df_item.rename(columns = {
        'locationCodeableConcept.coding.code': 'location_code',
        'locationCodeableConcept.coding.display': 'location_display'
        }, inplace = True)     
    
    if 'productOrService.coding' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('productOrService.coding')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        
        eob_df_item.rename(columns = {
        'productOrService.coding.code': 'product_code',
        'productOrService.coding.display': 'product_display'
        }, inplace = True)     
        
    if 'productOrService.extension' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('productOrService.extension')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        
        eob_df_item.rename(columns = {
        'productOrService.extension.valueCoding.code': 'product_ext_cd',
        'productOrService.extension.valueCoding.display': 'product_ext_display'
        }, inplace = True) 
        

    if 'modifier' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('modifier')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        eob_df_item = eob_df_item.explode('modifier.coding')
        eob_df_item = json_normalize(
        eob_df_item.to_dict(orient='records')
        )
        eob_df_item.rename(columns = {
        'modifier.coding.code': 'modifier_code',
        'modifier.coding.version': 'modifier_version'
        }, inplace = True) 
        


    if 'revenue.extension' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('revenue.extension')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        
        eob_df_item.rename(columns = {
        'revenue.extension.valueCoding.display': 'revenue_ext_display',
        'revenue.extension.valueCoding.code': 'revenue_ext_code'
        }, inplace = True)    

    if 'revenue.coding' in eob_df_item.columns:
        eob_df_item = eob_df_item.explode('revenue.coding')
        
        eob_df_item = json_normalize(
            eob_df_item.to_dict(orient='records')
        )
        
        eob_df_item.rename(columns = {
            'revenue.coding.code': 'revenue_code',
            'revenue.coding.display': 'revenue_display'
        }, inplace = True)
    eob_df_item.rename(columns = {
        'sequence': 'item_sequence'
    }, inplace = True)
    cols = [
        'Claim_ID',
        'item_sequence',
        'careTeamSequence',
        'diagnosisSequence',
        'product_code',
        'product_display',
        'product_ext_cd',
        'product_ext_display',
        'revenue_code',
        'revenue_display',
        'revenue_ext_code',
        'revenue_ext_display',
        'category_code',
        'category_display',
        'modifier_code',
        'modifier_version',
        'location_code',
        'location_display',
        'serviceperiod_start',
        'serviceperiod_end',
        'servicedDate',
        'created'
    ]
    eob_df_item = eob_df_item.reindex(columns = cols)
    eob_df_item['careTeamSequence'] = eob_df_item['careTeamSequence'].apply(lambda x: x[0] if isinstance(x, list) else x)
    eob_df_item['diagnosisSequence'] = eob_df_item['diagnosisSequence'].apply(lambda x: x[0] if isinstance(x, list) else x)
    eob_df_item = eob_df_item.drop_duplicates()
    eob_df_item = eob_df_item.reset_index(drop=True)
    eob_df_item['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item['bcda_file'] = file.name
    eob_df_item['extract_from'] = extract_from
    eob_df_item['extract_to'] = extract_to
    return eob_df_item

def eob_item_adjudication(eob_df, file):
    eob_df_item = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta = ['Claim_ID', 'created']
    )
    eob_df_item_adjudication = json_normalize(
        eob_df_item.to_dict(orient='records'),
        record_path='adjudication',
        meta = ['Claim_ID', 'sequence']
    )

    if 'reason.coding' in eob_df_item_adjudication.columns:
        eob_df_item_adjudication = eob_df_item_adjudication.explode('reason.coding')
        
        eob_df_item_adjudication = json_normalize(
            eob_df_item_adjudication.to_dict(orient='records')
        )
        
        eob_df_item_adjudication.rename(columns = {
        'reason.coding.code': 'reason_code',
        'reason.coding.display': 'reason_display'
        }, inplace = True)

    if 'category.coding' in eob_df_item_adjudication.columns:
        eob_df_item_adjudication = eob_df_item_adjudication.explode('category.coding')
        
        eob_df_item_adjudication = json_normalize(
            eob_df_item_adjudication.to_dict(orient='records')
        )
        
        eob_df_item_adjudication.rename(columns = {
        'category.coding.display': 'display',
        'category.coding.code': 'cat_code'
        }, inplace = True)
        

    if 'extension' in eob_df_item_adjudication.columns:
        eob_df_item_adjudication = eob_df_item_adjudication.explode('extension')
        
        eob_df_item_adjudication = json_normalize(
            eob_df_item_adjudication.to_dict(orient='records')
        )
        
        eob_df_item_adjudication.rename(columns = {
        'extension.valueCoding.code': 'LINE_PMT_80_100_CD',
        'extension.valueCoding.display': 'LINE_PMT_80_100_display'
        }, inplace = True)
    
    eob_df_item_adjudication.rename(columns = {
    'amount.currency': 'currency_type',
    'amount.value': 'amount',
    'sequence': 'item_sequence'
    }, inplace = True)

    cols = ['Claim_ID','item_sequence', 'cat_code', 'display', 'currency_type', 'amount', 'reason_code', 'reason_display', 'LINE_PMT_80_100_CD', 'LINE_PMT_80_100_display','created']
    eob_df_item_adjudication = eob_df_item_adjudication.reindex(columns=cols)
    eob_df_item_adjudication = eob_df_item_adjudication.drop_duplicates()
    eob_df_item_adjudication= eob_df_item_adjudication.reset_index(drop = True)
    eob_df_item_adjudication['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item_adjudication['bcda_file'] = file.name
    eob_df_item_adjudication['extract_from'] = extract_from
    eob_df_item_adjudication['extract_to'] = extract_to
    return eob_df_item_adjudication

def eob_item_ext(eob_df, file):
    eob_df_item_ext = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta=['Claim_ID', 'created']
    )
    if 'extension' in eob_df_item_ext.columns:
        eob_df_item_ext = eob_df_item_ext.explode('extension')
        
        eob_df_item_ext = json_normalize(
            eob_df_item_ext.to_dict(orient='records')
        )
        
    eob_df_item_ext.rename(columns = {
        'sequence': 'item_sequence'
    }, inplace = True)
    cols = [
        'Claim_ID',
        'item_sequence',
        'extension.url',
        'extension.valueCoding.code',
        'extension.valueCoding.system',
        'extension.valueQuantity.value',
        'extension.valueCoding.display',
        'extension.valueIdentifier.system',
        'extension.valueIdentifier.value',
        'extension.valueQuantity.code',
        'extension.valueQuantity.system',
        'extension.valueQuantity.unit',
        'created'
    ]

    eob_df_item_ext = eob_df_item_ext.reindex(columns=cols)
    eob_df_item_ext.columns = eob_df_item_ext.columns.str.replace('.', '_')
    eob_df_item_ext = eob_df_item_ext.drop_duplicates()
    eob_df_item_ext['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item_ext['bcda_file'] = file.name
    eob_df_item_ext['extract_from'] = extract_from
    eob_df_item_ext['extract_to'] = extract_to
    return eob_df_item_ext


def eob_item_loc_ext(eob_df, file):
    eob_df_item_loc_ext = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='item',
        meta=['Claim_ID', 'created']
    )
    if 'locationCodeableConcept.extension' in eob_df_item_loc_ext.columns:
        eob_df_item_loc_ext = eob_df_item_loc_ext.explode('locationCodeableConcept.extension')
        
        eob_df_item_loc_ext = json_normalize(
            eob_df_item_loc_ext.to_dict(orient='records')
        )
    eob_df_item_loc_ext.rename(columns = {
        'sequence': 'item_sequence'
    }, inplace = True)
    cols = [
        'Claim_ID',
        'item_sequence',
        'locationCodeableConcept.extension.url',
        'locationCodeableConcept.extension.valueCoding.code',
        'locationCodeableConcept.extension.valueCoding.system',
        'locationCodeableConcept.extension.valueCoding.display',
        'locationCodeableConcept.extension.valueIdentifier.system',
        'locationCodeableConcept.extension.valueIdentifier.value',
        'created'
    ]

    eob_df_item_loc_ext = eob_df_item_loc_ext.reindex(columns=cols)
    eob_df_item_loc_ext.columns = eob_df_item_loc_ext.columns.str.replace('.', '_')
    eob_df_item_loc_ext = eob_df_item_loc_ext.drop_duplicates()
    eob_df_item_loc_ext['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_item_loc_ext['bcda_file'] = file.name
    eob_df_item_loc_ext['extract_from'] = extract_from
    eob_df_item_loc_ext['extract_to'] = extract_to
    return eob_df_item_loc_ext


def eob_type(eob_df, file):
    eob_df_type = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='type_coding',
        meta=['Claim_ID', 'created']
    )
    eob_df_type.rename(columns = {
        'code': 'clm_type_cd',
    }, inplace= True)
    eob_df_type = eob_df_type[['Claim_ID', 'clm_type_cd', 'display', 'system', 'created']]
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
    meta=['Claim_ID', 'created']
    )

    eob_total_category = eob_total.explode('category.coding')

    eob_total_category = json_normalize(
        eob_total_category.to_dict(orient='records')
        , meta = ['Claim_ID', 'created']
    )
    eob_total_category.rename(columns= {
        'amount.currency': 'currency_type',
        'amount.value': 'amount',
        'category.coding.display': 'category'
    }, inplace = True)
    eob_total_category = eob_total_category[['Claim_ID', 'category', 'currency_type', 'amount', 'created']]
    eob_total_category['bcda_extract_date'] = pd.Timestamp.today()
    eob_total_category['bcda_file'] = file.name
    eob_total_category['extract_from'] = extract_from
    eob_total_category['extract_to'] = extract_to
    return eob_total_category

def eob_contained(eob_df, file):
    if 'contained' in eob_df.columns:        
        eob_df_contained = json_normalize(
            eob_df.to_dict(orient='records'),
            record_path='contained',
            meta = ['Claim_ID', 'created']
        )
        if 'identifier' in eob_df_contained.columns:
            eob_df_contained = eob_df_contained.explode('identifier')



            eob_df_contained = json_normalize(
                eob_df_contained.to_dict(orient='records')
            )
            eob_df_contained = eob_df_contained.explode('identifier.type.coding')
            eob_df_contained = json_normalize(
                eob_df_contained.to_dict(orient='records')
            )

            eob_df_contained.rename(columns = {'identifier.value':'NPI', 'identifier.type.coding.code': 'type_code'}, inplace = True)
        
        if 'code.coding' in eob_df_contained.columns:
            eob_df_contained = eob_df_contained.explode('code.coding')
            
            eob_df_contained = json_normalize(
                eob_df_contained.to_dict(orient='records')
            )
            eob_df_contained.rename(columns = {
                'valueQuantity.value': 'results',
                'code.coding.code': 'test_cd',
                'code.coding.display': 'test_display'
            }, inplace=True)
        
        
        cols = [
            'Claim_ID',
            'id',
            'type_code',
            'NPI', 
            'active', 
            'name', 
            'test_cd',
            'test_display',
            'results',
            'resourceType',
            'created'
        ]
        
        eob_df_contained = eob_df_contained.reset_index(drop = True)
        eob_df_contained = eob_df_contained.reindex(columns = cols)
        eob_df_contained['bcda_extract_date'] = pd.Timestamp.today()
        eob_df_contained['bcda_file'] = file.name
        eob_df_contained['extract_from'] = extract_from
        eob_df_contained['extract_to'] = extract_to
    else:
        eob_df_contained= pd.DataFrame(columns = [
            'Claim_ID',
            'id',
            'type_code',
            'NPI',
            'active',
            'name',
            'test_cd',
            'test_display',
            'results',
            'resourceType',
            'created',
            'bcda_extract_date',
            'bcda_file',
            'extract_from',
            'extract_to'
        ])
    return eob_df_contained

def eob_supporting(eob_df, file):

    # Step 1: normalize supportingInfo
    eob_df_sup = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path='supportingInfo',
        meta=['Claim_ID', 'created']
    )

    # Step 2: explode category
    eob_df_sup = eob_df_sup.explode('category.coding')

    eob_df_sup = json_normalize(
        eob_df_sup.to_dict(orient='records')
    )

    # Step 3: explode code
    if 'code.coding' in eob_df_sup.columns:
        eob_df_sup = eob_df_sup.explode('code.coding')

        eob_df_sup = json_normalize(
            eob_df_sup.to_dict(orient='records')
        )

    # Step 4: standardize column names
    eob_df_sup.columns = eob_df_sup.columns.str.replace('.', '_', regex=False)

    # Step 5: rename
    eob_df_sup = eob_df_sup.rename(columns={
        'sequence': 'Supp_Sequence'
    })

    # Step 6: define columns
    cols = [
        "Claim_ID",
        "Supp_Sequence",

        "timingDate",
        "timingPeriod_start",
        "timingPeriod_end",

        "category_coding_code",
        "category_coding_display",
        "category_coding_system",

        "code_coding_code",
        "code_coding_display",
        "code_coding_system",

        "valueQuantity_value",
        "valueQuantity_unit",
        "valueQuantity_code",
        "valueQuantity_system",

        "valueReference_reference",
        
        'created'
    ]

    # Step 7: ensure all columns exist
    for c in cols:
        if c not in eob_df_sup.columns:
            eob_df_sup[c] = None

    # Step 8: select columns
    eob_df_sup = eob_df_sup[cols]
    eob_df_sup = eob_df_sup[
    ~eob_df_sup['category_coding_code'].isin(['info', 'clmrecvddate'])
    ]
    
    # Step 9: metadata
    eob_df_sup['bcda_extract_date'] = pd.Timestamp.today()
    eob_df_sup['bcda_file'] = file.name
    eob_df_sup['extract_from'] = extract_from
    eob_df_sup['extract_to'] = extract_to

    return eob_df_sup

def eob_procedure(eob_df, file):
    if 'procedure' in eob_df.columns:    
        eob_df_procedure = json_normalize(
            eob_df.to_dict(orient='records'),
            record_path='procedure',
            meta = ['Claim_ID', 'created']
            
        )
        eob_df_procedure_2 = eob_df_procedure.explode('procedureCodeableConcept.coding')
        
        eob_df_procedure_2 =json_normalize(
            eob_df_procedure_2.to_dict(orient='records')
        )
        eob_df_procedure_2.rename(columns ={
            'procedureCodeableConcept.coding.code': 'procedure_code',
            'procedureCodeableConcept.coding.display': 'display'
        }, inplace = True)
        cols = [
            'Claim_ID',
            'sequence',
            'procedure_code',
            'display',
            'date',
            'created'
        ]
        eob_df_procedure_2 = eob_df_procedure_2.reindex(columns=cols)
        if 'display' in eob_df_procedure_2.columns:
            eob_df_procedure_2['display'] = eob_df_procedure_2['display'].astype(str).str.replace('"', '', regex=False)
        eob_df_procedure_2 = eob_df_procedure_2.drop_duplicates()
        eob_df_procedure_2['bcda_extract_date'] = pd.Timestamp.today()
        eob_df_procedure_2['bcda_file'] = file.name
        eob_df_procedure_2['extract_from'] = extract_from
        eob_df_procedure_2['extract_to'] = extract_to
    else:
        eob_df_procedure_2 = pd.DataFrame(columns=[
            'Claim_ID', 'sequence', 'procedure_code', 'display', 'date','created'
            'bcda_extract_date', 'bcda_file', 'extract_from', 'extract_to'
        ])
    return eob_df_procedure_2

def eob_identifier(eob_df, file):
    if 'identifier' in eob_df.columns:
        eob_df_identifier = json_normalize(
        eob_df.to_dict(orient='records'),
        record_path=['identifier'],
        meta=['Claim_ID', 'created']
        )
        eob_df_identifier=eob_df_identifier.explode('type.coding')
        eob_df_identifier=json_normalize(
            eob_df_identifier.to_dict(orient='records')
        )
        eob_df_identifier = eob_df_identifier[eob_df_identifier['system']== r'https://bluebutton.cms.gov/resources/identifier/claim-group']
        eob_df_identifier.rename(columns={
            'value':'Claim_group'
        }, inplace = True)
        eob_df_identifier= eob_df_identifier[['Claim_ID', 'Claim_group']]
        eob_df_identifier['bcda_extract_date'] = pd.Timestamp.today()
        eob_df_identifier['bcda_file'] = file.name
        eob_df_identifier['extract_from'] = extract_from
        eob_df_identifier['extract_to'] = extract_to
    else:
        eob_df_identifier = pd.DataFrame(columns=[
            'Claim_ID', 'Claim_group', 'bcda_extract_date', 'bcda_file', 'extract_from', 'extract_to'
        ])
    return eob_df_identifier


def load_to_sql(df, table_name, file_name, engine):

    df = df.copy()

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=5000
        )

    except Exception as e:
        print(f"SQL INSERT FAILED {file_name}")
        print(e)
        raise

def process_file(file):
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
    item_df = eob_item(eob_df, file)
    print(f'item done {file}')
    item_adj_df = eob_item_adjudication(eob_df, file)
    print(f'item_adj done {file}')
    item_ext_df = eob_item_ext(eob_df, file)
    print(f'item_ext done {file}')
    item_loc_ext_df = eob_item_loc_ext(eob_df, file)
    print(f'item_loc_ext done {file}')
    type_df = eob_type(eob_df, file)
    print(f'type done {file}')
    total_df = eob_total(eob_df, file)
    print(f'total done {file}')
    contained_df = eob_contained(eob_df, file)
    print(f'contained done {file}')
    supporting_df = eob_supporting(eob_df, file)
    print(f'supporting done {file}')
    procedure_df = eob_procedure(eob_df, file)
    print(f'procedure done {file}')
    identifier_df = eob_identifier(eob_df, file)
    print(f'identifier done {file}')
    
    load_to_sql(basetable_df, 'EOB_Basetable_Staging', file.name, engine)
    del basetable_df
    print(f'EOB_Basetable_Staging loaded {file}')
    load_to_sql(diagnosis_df, 'EOB_Diagnosis_Staging', file.name, engine)
    del diagnosis_df
    print(f'EOB_Diagnosis_Staging loaded {file}')
    load_to_sql(BenefitBalance_df,'EOB_BenefitBalance_Staging', file.name, engine)
    del BenefitBalance_df
    print(f'EOB_BenefitBalance_Staging loaded {file}')
    load_to_sql(careteam_df,'EOB_careteam_Staging', file.name, engine)
    del careteam_df
    print(f'EOB_careteam_Staging loaded {file}')
    load_to_sql(item_df, 'EOB_item_Staging', file.name, engine)
    del item_df
    print(f'EOB_item_Staging loaded {file}')
    load_to_sql(item_adj_df, 'EOB_item_adj_Staging', file.name, engine)
    del item_adj_df
    print(f'EOB_item_adj_Staging loaded {file}')
    load_to_sql(item_ext_df, 'EOB_item_ext_Staging', file.name, engine)
    del item_ext_df
    print(f'EOB_item_ext_Staging loaded {file}')
    load_to_sql(item_loc_ext_df, 'EOB_item_loc_ext_Staging', file.name, engine)
    del item_loc_ext_df
    print(f'EOB_item_loc_ext_Staging loaded {file}')
    load_to_sql(type_df, 'EOB_type_Staging', file.name, engine)
    del type_df
    print(f'EOB_type_Staging loaded {file}')
    load_to_sql(total_df, 'EOB_total_Staging', file.name, engine)
    del total_df
    print(f'EOB_total_Staging loaded {file}')
    load_to_sql(contained_df,'EOB_contained_Staging', file.name, engine)
    del contained_df
    print(f'EOB_contained_Staging loaded {file}')
    load_to_sql(supporting_df,'EOB_supporting_Staging', file.name, engine)
    del supporting_df
    print(f'EOB_supporting_Staging loaded {file}')
    load_to_sql(procedure_df,'EOB_procedure_Staging', file.name, engine)
    del procedure_df
    print(f'EOB_procedure_Staging loaded {file}')
    load_to_sql(identifier_df,'EOB_identifier_Staging', file.name, engine)
    del identifier_df
    print(f'EOB_identifier_Staging loaded {file}')
    
    print(f'Loaded {file} to database')    
    
def main():
    
    tables = ['EOB_Basetable_Staging',
            'EOB_Diagnosis_Staging',
            'EOB_BenefitBalance_Staging',
            'EOB_careteam_Staging',
            'EOB_item_Staging',
            'EOB_item_adj_Staging',
            'EOB_item_ext_Staging',
            'EOB_item_loc_ext_Staging',
            'EOB_type_Staging',
            'EOB_total_Staging',
            'EOB_contained_Staging',
            'EOB_supporting_Staging',
            'EOB_procedure_Staging',
            'EOB_identifier_Staging'
            ]
    
    for table in tables:
        truncate_stging_tables(table)
    
    files = filepath.glob('ExplanationOfBenefit_*.ndjson')

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_file, f) for f in files]

        for f in futures:
            f.result()

    engine.dispose()

if __name__ == "__main__":
    main()
