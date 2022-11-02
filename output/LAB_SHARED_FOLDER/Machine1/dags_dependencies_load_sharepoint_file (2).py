from O365 import Account, FileSystemTokenBackend
import json
import requests
import os
import warnings
import numpy as np
import pandas as pd
from uuid import uuid4
from google.cloud import secretmanager
# warnings.filterwarnings("ignore", category=DeprecationWarning)

project_id='entegris-p-dao-sc2'

def access_secret_version(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name)
    # if package version > 1.0.0 use this
    # response = client.access_secret_version(request={"name": name})

    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    return payload


def get_credentials():
    client_id = access_secret_version(project_id,'O365_CLIENT_ID','latest')
    client_secret = access_secret_version(project_id,'O365_CLIENT_SECRET','latest')
    credentials = (client_id, client_secret)
    return credentials


def get_token(credentials, token_path):
    tenant_id = access_secret_version(project_id,'TENANT_ID','latest')
    token_filename = 'token-{}.txt'.format(uuid4())
    token_backend = FileSystemTokenBackend(token_path=token_path, token_filename=token_filename)

    account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id,token_backend=token_backend)
    if account.authenticate():
        print('Authenticated!')

    with open(token_path + token_filename) as token_file:
        token = json.load(token_file)['access_token']
    os.remove(token_path+token_filename)
    return token



def get_data(list_id,bq_columns_list):
    site_id = access_secret_version(project_id,'SITE_ID','latest')
    credentials = get_credentials()
    # token_path='/Users/pkattamuri/Downloads/'
    token_path='/home/airflow/gcs/data/'
    token = get_token(credentials, token_path)
    headers = {'Authorization': 'Bearer {}'.format(token)}

    columns_url="https://graph.microsoft.com/v1.0/sites/"+site_id+"/lists/"+list_id+"/columns"
    columns_response=requests.get(columns_url, headers=headers).json()
    columns={}
    col_dtypes = {}
    for item in columns_response['value']:
        if item['displayName'] in bq_columns_list:
            columns[item['name']] = item['displayName'].replace(' ','_').replace('-','_').replace('.','')
            if 'dateTime' in item.keys():
                # if item['dateTime']['format']=='dateTime':
                col_dtypes[item['name']]= 'datetime'
                # elif item['dateTime']['format']=='dateOnly':
            elif 'number' in item.keys():
                col_dtypes[item['name']]= 'float'
            elif 'text' in item.keys():
                col_dtypes[item['name']]= 'string'
            else:
                columns.pop(item['name'],None)
                col_dtypes[item['name']]= 'unknown'
                print('Handle this')

    print(columns)

    data_url = "https://graph.microsoft.com/v1.0/sites/"+site_id+"/lists/"+list_id+"/items?expand=columns,items(expand=fields)"
    data_response = requests.get(data_url, headers=headers).json()
    # print(data_response)

    tmp=[]
    for item in data_response['value']:
        tmp.append(item['fields'])
    
    df = pd.read_json(json.dumps(tmp), orient='records')
    df = df.filter(items=columns.keys())
    for col in columns.keys():
        if col not in df.columns:
            df[col] = pd.Series(dtype=col_dtypes[col])
    for col in df.columns:
        if col_dtypes[col]=='datetime':
            df[col]=pd.to_datetime(df[col])
    df = df.rename(columns=columns)
    df = df.replace('null', np.nan)
    
    return df


def supplier_survey_file():
    supplier_survey_v2_id = 'dabe938d-4bed-48e1-8579-c14d0589d3ee'
    # supplier_survey_bq_columns_list=[
    #     'Title',
    #     'field_2',
    #     'field_3',
    #     'field_4',
    #     'field_5',
    #     'field_6',
    #     'field_7',
    #     'field_8',
    #     'field_9',
    #     'field_10',
    #     'field_11',
    #     'field_12',
    #     'field_13',
    #     'field_14',
    #     'field_15',
    #     'field_16',
    #     'field_17',
    #     'field_18',
    #     'field_19',
    #     'field_20',
    #     'field_21',
    #     'field_22',
    #     'field_23',
    #     'QuarterStartDate'
    # ]
    supplier_survey_bq_columns_list=[
        'Title',
        'Completion time',
        'Email User',
        'Name',
        'Material',
        'Vendor',
        'Field_1',
        'Field_23',
        'Field_7',
        'Field_16',
        'Field_8',
        'Field_17',
        'Field_18',
        'Field_19',
        'Field_24',
        'Field_25',
        'Field_26',
        'Field_27',
        'Field_28',
        'Field_29',
        'Field_30',
        'Field_31',
        'Field_32',
        'Quarter Start Date'
    ]
    bq_df = get_data(supplier_survey_v2_id, supplier_survey_bq_columns_list)
    bq_df.to_gbq("sharepoint_raw_layer.supplier_survey", project_id=project_id, if_exists='replace')

def production_survey_file():
    production_survey_v2_id = '7438b1db-50ea-4578-a343-10a648a9a4e4'
    # production_survey_bq_columns_list= [
    #     'Title',
    #     'field_2',
    #     'field_3',
    #     'field_4',
    #     'field_5',
    #     'field_6',
    #     'field_7',
    #     'field_8',
    #     'field_9',
    #     'field_10',
    #     'field_11',
    #     'field_12',
    #     'field_13',
    #     'field_14',
    #     'field_15',
    #     'field_16',
    #     'field_17',
    #     'field_18',
    #     'QuarterStartDate',
    #     'WhatDivisionisthisproductapartof',
    #     'WhatKAMcustomersisthisproductsol',
    #     'HowmanyfinishedmaterialSPCcharts',
    #     'ProductionPlant'
    # ]
    production_survey_bq_columns_list = [
        'Title',
        'Completion time',
        'Email User',
        'Name',
        'Product_Family',
        'Field_38',
        'Field_50',
        'Field_51',
        'Field_52',
        'AMLC_Enrollment',
        'Field_53',
        'BCP_Enrollment',
        'Field_55',
        'Field_56',
        'Field_59',
        'Field_60',
        'Field_61',
        'Field_62',
        'Quarter Start Date',
        'Division',
        'KAMs',
        'Num_Charts_woRFCs',
        'Production Plant'
    ]
    bq_df = get_data(production_survey_v2_id, production_survey_bq_columns_list)
    bq_df.to_gbq("sharepoint_raw_layer.production_survey", project_id=project_id, if_exists='replace')


def parameters_data_table_file():
    parameters_data_table_id = 'a7ab42bb-2d64-470a-ba28-e3d81f71339c'
    # parameters_data_table_bq_columns_list= [
    #     'Title',
    #     'field_1',
    #     'field_2',
    #     'field_3',
    #     'field_4',
    #     'field_5',
    #     'field_6',
    #     'field_7',
    #     'field_8',
    #     'field_9',
    #     'field_10',
    #     'field_11',
    #     'field_12',
    #     'field_13',
    #     'field_14',
    #     'field_15',
    #     'field_16',
    #     'field_17',
    #     'field_18',
    #     'field_19',
    #     'field_20',
    #     'field_21',
    #     'field_22',
    #     'field_23',
    #     'field_24',
    #     'field_25'
    # ]
    parameters_data_table_bq_columns_list = [
        'Title',
        'Material Family',
        'Material',
        'Material Description',
        'Input or Output Variable',
        'Camline Server',
        'Parameter',
        'Cust_Cntl_Limit_Channel',
        'SPC_Channel',
        'CZRQ_Channel',
        'IDL_Channel',
        'Supplier',
        'Site',
        'Key Ctrl',
        'Calc. Method',
        'MDL Date',
        'GRR Date',
        'GRR Freq',
        'Metrology Backup',
        'Metrology BCP',
        'LTS CLCR',
        'MDL',
        'UCL',
        'LCL',
        'USL',
        'Target',
        'LSL',
        'Units',
        'Meas Standard Dev',
        'TMM',
        'Normalized Center',
        'Historical Mean'
    ]
    bq_df = get_data(parameters_data_table_id, parameters_data_table_bq_columns_list)
    bq_df.to_gbq("sharepoint_raw_layer.parameters_data_table", project_id=project_id, if_exists='replace')


if __name__=="__main__":
    # pass
    production_survey_file()
