import psycopg2
import pandas as pd
import numpy as np
import os


dbname = 'plant_database'
user = 'user'
host = '172.27.0.2'
password = "pass"
port = 5432

db_params = {'dbname':dbname, 'user':user, 'host':host, 'password':password, 'port':port}



def make_sql_req(sql_req):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute(sql_req)
        conn.commit()
        count = cursor.rowcount
        cursor.close()
        conn.close()
    except: pass

        
def create_tab(df, dtypes_dict, table_name):
    dtypes_ = df.dtypes
    dtypes_ = dtypes_.astype(str).map(dtypes_dict).reset_index().values
    sql_req = f'CREATE TABLE {table_name} ('+', '.join([f'"{i[0]}"'+' '+i[1] for i in dtypes_]) + ')'
    make_sql_req(sql_req)


def insert_init_data(df, table_name):
    df = df.fillna('NULL')
    df = df.astype(str)
    cols = '(' + ', '.join([f'"{str(i)}"' for i in df.columns]) + ')'
    vals = ', '.join([', '.join(str(i).split(' ')) for i in df.values])
    vals = vals.replace('[', '(').replace(']', ')')
    vals = vals.replace("'NULL'", 'NULL')
    sql_req = f"INSERT INTO {table_name} {cols} VALUES {vals};"
    make_sql_req(sql_req)
    
def set_primary_key(key_col, table_name):
    sql_req = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({key_col});"
    make_sql_req(sql_req)
    
    
def set_foreign_key(constraint_name, col_source, col_foreign, table_source, table_foreign):
    sql_req = f"""ALTER TABLE {table_foreign}
    ADD CONSTRAINT {constraint_name} 
    FOREIGN KEY ({col_foreign}) 
    REFERENCES {table_source} ({col_source});"""
    make_sql_req(sql_req)
    
    
dtypes_dict = {'object':'varchar', 'int64':'int', 'int32':'int',\
               'datetime64[ns]':'timestamp', 'float64':'numeric'}


make_sql_req('DROP SCHEMA raw_layer CASCADE')
make_sql_req('DROP SCHEMA operational_layer CASCADE')
make_sql_req('CREATE SCHEMA raw_layer')
make_sql_req('CREATE SCHEMA operational_layer')



df0 = pd.read_csv('/home/task1/files/data0.csv', sep=';')
df0['DataGetParammetrs'] = pd.to_datetime(df0['DataGetParammetrs'])
df1 = pd.read_csv('/home/task1/files/data1.csv', sep=';')
df2 = pd.read_csv('/home/task1/files/data2.csv', sep=';')

# df7 = pd.read_csv('Option_2/data/data7.csv', sep=';')
df_bad = pd.read_csv('/home/task1/files/data_bad.csv', sep=';')
df_bad['DataGetParammetrs'] = pd.to_datetime(df_bad['DataGetParammetrs'])

df_data = df0.append(df1, ignore_index = True)\
            .append(df2, ignore_index = True)\
            .append(df_bad, ignore_index = True)


df_data['Company_name'] = None
company_names = ['Bosch', 'Siemens', 'Festo', 'NewGen', 'Versa', 'Fanuc', 'SensXiom', 'OSISoft']
for cn in company_names:
    df_data.loc[df_data['Customer'].str.lower().str.contains(cn.lower()), 'Company_name'] = cn
df_data.loc[df_data['Company_name'].isna(), 'Company_name'] = 'Other'

machine_types = ['PLC', 'CNC', 'IOT']
df_data['eq_type'] = None
for mt in machine_types:
    df_data.loc[df_data['Customer'].str.lower().str.contains(mt.lower()), 'eq_type'] = mt
df_data.loc[df_data['eq_type'].isna(), 'eq_type'] = 'Other'



customer_cols = ['Customer', 'Location', 'Company_name']
df_customers = df_data[customer_cols].drop_duplicates()
df_customers['id_customer'] = np.arange(len(df_customers))


product_cols = ['Number of sensors', 'Power', 'Wi-Fi', 'BT', 'JTAG', 'Board', 'iOSapp', 'AndroidApp']
df_product = df_data[product_cols].drop_duplicates()
df_product['id_product'] = np.arange(len(df_product))


df_data = df_data.merge(df_customers).merge(df_product)
meas_cols = ['id_customer', 'id_product', 'DataGetParammetrs', 'TempSen', 'HumpSen', 'LightSen', \
                 'AcousticsSen', 'AccSen', 'GyrSen', 'MagSen', 'All Time Work']
df_meas = df_data[meas_cols].drop_duplicates()
df_meas['id_measurment'] = np.arange(len(df_meas))
df_data = df_data.drop(columns = ['id_product', 'id_customer'])

customer_cols = ['id_customer'] + customer_cols
product_cols = ['id_product'] + product_cols
meas_cols = ['id_measurment'] + meas_cols

df_customers = df_customers[customer_cols]
df_product = df_product[product_cols]
df_meas = df_meas[meas_cols]


df_customers_op = df_customers.copy()
df_customers_op['customer_data_quality_status'] = None
df_product_op = df_product.copy()
df_product_op['product_data_quality_status'] = None
df_meas_op = df_meas.copy()
df_meas_op['measurment_data_quality_status'] = None



create_tab(df_customers, dtypes_dict, 'raw_layer.customers')
create_tab(df_product, dtypes_dict, 'raw_layer.products')
create_tab(df_meas, dtypes_dict, 'raw_layer.measurments')

create_tab(df_customers_op, dtypes_dict, 'operational_layer.customers')
create_tab(df_product_op, dtypes_dict, 'operational_layer.products')
create_tab(df_meas_op, dtypes_dict, 'operational_layer.measurments')


insert_init_data(df_customers, 'raw_layer.customers')
insert_init_data(df_product, 'raw_layer.products')
insert_init_data(df_meas, 'raw_layer.measurments')



set_primary_key('id_customer', 'raw_layer.customers')
set_primary_key('id_product', 'raw_layer.products')
set_primary_key('id_measurment', 'raw_layer.measurments')

set_primary_key('id_customer', 'operational_layer.customers')
set_primary_key('id_product', 'operational_layer.products')
set_primary_key('id_measurment', 'operational_layer.measurments')

set_foreign_key('customer_fk', col_source='id_customer', col_foreign='id_customer',\
               table_source='raw_layer.customers', table_foreign='raw_layer.measurments')

set_foreign_key('product_fk', col_source='id_product', col_foreign='id_product',\
               table_source='raw_layer.products', table_foreign='raw_layer.measurments')


set_foreign_key('customer_fk', col_source='id_customer', col_foreign='id_customer',\
               table_source='operational_layer.customers', table_foreign='operational_layer.measurments')

set_foreign_key('product_fk', col_source='id_product', col_foreign='id_product',\
               table_source='operational_layer.products', table_foreign='operational_layer.measurments')


