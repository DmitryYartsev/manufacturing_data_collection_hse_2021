import pandas as pd
import numpy as np
import os
import psycopg2
import time


dbname = 'plant_database'
user = 'user'
host = '172.27.0.2'
password = "pass"
port = 5432

db_params = {'dbname':dbname, 'user':user, 'host':host, 'password':password, 'port':port}

numeric_feats = ['Number of sensors', 'TempSen', 'HumpSen', 'LightSen', 'AcousticsSen', 'AccSen', 'GyrSen', 'MagSen']
binary_cols = ['Wi-Fi', 'BT', 'JTAG', 'iOSapp', 'AndroidApp']


def make_sql_req(sql_req):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute(sql_req)
        conn.commit()
        count = cursor.rowcount
        cursor.close()
        conn.close()
    except:
        print(sql_req)
        raise Exception('Error in db')
        
        
def insert_init_data(df, table_name):
    df = df.fillna('NULL')
    df = df.astype(str)
    cols = '(' + ', '.join([f'"{str(i)}"' for i in df.columns]) + ')'
    vals = ', '.join([', '.join(str(i).split(' ')) for i in df.values])
    vals = vals.replace('[', '(').replace(']', ')')
    vals = vals.replace("'NULL'", 'NULL')
    sql_req = f"INSERT INTO {table_name} {cols} VALUES {vals};"
    make_sql_req(sql_req)
    
    
def categorical_cleaning(df, col):
    if 'product_data_quality_status' not in df.columns:
        df['product_data_quality_status'] = 'good'
    df[col] = df[col].str.lower().str.strip().str.strip(';')
    df.loc[df[col].str.contains('no inf').fillna(False), col]  = None
    df.loc[df[col].str.contains('n/i').fillna(False), col]  = None
    df.loc[df[col]=='', col]  = None
    df.loc[df[col]=='нет', col]  = 'no'
    df.loc[df[col]=='да', col]  = 'yes'
    df.loc[df[col]=='0', col]  = 'no'
    df.loc[df[col]=='1', col]  = 'yes'
    df.loc[df[col]=='+', col]  = 'yes'
    df.loc[df[col]=='-', col]  = 'no'
    df.loc[df[col]=='--', col]  = 'no'
    df.loc[~df[col].isin(['yes', 'no']), col] = None
    df.loc[~df[col].isin(['yes', 'no']), 'product_data_quality_status'] = 'bad'
    return df


    

if __name__=='__main__':
    while True:
        try:
            conn = psycopg2.connect(**db_params) 

            df_customers = pd.read_sql('select * from raw_layer.customers', conn)
            df_products = pd.read_sql('select * from raw_layer.products', conn)
            df_meas = pd.read_sql('select * from raw_layer.measurments', conn)

            id_customer_done = pd.read_sql('select id_customer from operational_layer.customers', conn)['id_customer']
            id_product_done = pd.read_sql('select id_product from operational_layer.products', conn)['id_product']
            id_measurment_done = pd.read_sql('select id_measurment from operational_layer.measurments', conn)['id_measurment']

            conn.close()

            df_customers = df_customers[~df_customers['id_customer'].isin(id_customer_done)]
            df_products = df_products[~df_products['id_product'].isin(id_product_done)]
            df_meas = df_meas[~df_meas['id_measurment'].isin(id_measurment_done)]


            # customers
            df_customers['customer_data_quality_status'] = None
            df_customers.loc[df_customers['Customer'].isna(), 'customer_data_quality_status'] = 'bad'
            df_customers.loc[df_customers['Location'].isna(), 'customer_data_quality_status'] = 'bad'
            df_customers.loc[df_customers['Company_name'].isna(), 'customer_data_quality_status'] = 'bad'
            df_customers['customer_data_quality_status'] = df_customers['customer_data_quality_status'].fillna('good')


            ## products
            df_products['product_data_quality_status'] = 'good'
            df_products['original_Board'] = df_products['Board']
            df_products['Board'] = df_products['Board'].str.strip(';').str.strip(' ').str.strip('"').str.strip(',')
            df_products['Board'] = df_products['Board'].str.strip(';').str.strip(' ').str.strip('"').str.strip(',')
            df_products.loc[(df_products['Board']=='').fillna(True), 'Board'] = None
            df_products.loc[df_products['original_Board']!=df_products['Board'], 'product_data_quality_status'] = 'bad'
            df_products = df_products.drop(columns = ['original_Board'])

            for col in binary_cols:
                df_products = categorical_cleaning(df_products, col)


            ## measurment
            df_meas['measurment_data_quality_status'] = 'good'    


            if len(df_customers)!=0: insert_init_data(df_customers, 'operational_layer.customers')
            if len(df_products)!=0: insert_init_data(df_products, 'operational_layer.products')
            if len(df_meas)!=0: insert_init_data(df_meas, 'operational_layer.measurments')    
        except:
            time.sleep(1)
            print('Bad iteration')
    time.sleep(5)
