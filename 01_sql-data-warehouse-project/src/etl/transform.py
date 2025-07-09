import os
import pandas as pd
import numpy as np
from src.utils.logger import get_logger

logger = get_logger(__name__)
def transform_data(function_dict, src_file, dest_file):
  logger.info(f"Processing {src_file}")
  df = pd.read_csv(src_file, index_col=False)
  """DO basic cleaning
  1. remove white space in cell
  2. remove white space in column
  """
  df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
  df.columns = df.columns.str.strip()

  """transform data for each files, use specific function for each file"""
  processed_df = function_dict.get(os.path.basename(src_file))(df)

  """re check the transform process"""
  null_counts = processed_df.isnull().sum().sum()
  if null_counts !=0:
    raise ValueError(f"Null values are found after processing file {src_file}")
  
  """Save processed file"""
  processed_df.to_csv(dest_file, index=False)

def transform_customer_order_info(df):
  logger.info("Processing customer order information")
  """Remove duplicated rows"""
  df['rank'] = df.groupby('cst_id')["cst_create_date"].rank(method='dense', ascending=False)
  df = df.loc[df['rank']==1]
  df.drop('rank', axis=1, inplace=True)

  """id must be interger"""
  df['cst_id'] = df['cst_id'].astype(int)
  """convert 'F' to 'Female', 'M' to 'Male' and 'N/A' for others convert 'S' to 'Single', 'M' to 'Married'"""
  df.cst_gndr = df.cst_gndr.replace({'F':'Female', 'M': 'Male', np.nan: 'N/A'  })
  df.cst_marital_status = df.cst_marital_status.replace({'M': 'Married', 'S': 'Single'})

  return df

def transform_product_info(df):
  df.prd_cost = df.prd_cost.fillna(0)
  df.prd_line = df.prd_line.replace({
    'M': 'Mountain',
    'R':'Road',
    'S':'Other sales',
    'T': 'Touring',
    np.nan : 'N/A'
  })
  df.prd_end_dt = df.prd_end_dt\
                        .fillna(df.prd_start_dt.shift(-1))\
                        .fillna(df.prd_start_dt.shift(1) ) #if the last row is empty, use the previous row
  df[['prd_cat_id', 'prd_sub_key']]=df.prd_key.str.extract(r'(\w+-\w+)-(.*)')
  df.prd_cat_id = df.prd_cat_id.str.replace("-","_")

  return df

def transform_sale_info(df):
  df['sls_price'] = abs(df.sls_price)

  # if sales price is zero or null, calculate using sales and quantity
  df['sls_price'] = np.where(df.sls_price.isnull(), df.sls_sales/df.sls_quantity, df.sls_price)
  # if sales is negative, recalculate from quantity and price
  df['sls_sales'] = np.where((df['sls_sales'] <= 0) | (df['sls_sales'].isnull()), df.sls_quantity*df.sls_price, df.sls_sales)
  
  #convert to datetime
  df['sls_ship_dt']=pd.to_datetime(df.sls_ship_dt, format='%Y%m%d')
  df['sls_due_dt'] = pd.to_datetime(df.sls_due_dt, format='%Y%m%d')
  df['sls_order_dt'] = pd.to_datetime(df.sls_order_dt, format='%Y%m%d', errors='coerce')

  df.sls_order_dt =df.sls_order_dt.fillna(df['sls_ship_dt'] + pd.Timedelta(days=-1))

  return df

def transform_customer_info(df):
  df.CID=df.CID.replace(r'^NAS','', regex=True)
  df.GEN = df.GEN.replace({'F':'Female', 'M':'Male', np.nan:'N/A', '':'N/A'})

  return df

def transform_location_info(df):
  df.CID = df.CID.str.replace('-','')
  df.CNTRY = df.CNTRY.replace({
    np.nan: 'N/A',
    '':'N/A',
    'DE':'Germany',
    'US':'United States',
    'USA':'United States'})

  return df

def transform_category_info(df):
  df.loc[len(df)]={"ID":"CO_PE", "CAT":"Components", "SUBCAT":"Wheels", "MAINTENANCE":"No"}
  return df

def transform_datasets(**config):
  function_dict ={
    "cust_info.csv": transform_customer_order_info,
    "prd_info.csv": transform_product_info,
    "sales_details.csv":transform_sale_info,
    "CUST_AZ12.csv": transform_customer_info,
    "LOC_A101.csv": transform_location_info,
    "PX_CAT_G1V2.csv": transform_category_info
  }
  dest_folder = config['processed_dataset']['dest_folder']
  os.makedirs(dest_folder, exist_ok=True)
  for child_key, child_values in config['processed_dataset']['child_items'].items():
    sub_folder = os.path.join(dest_folder, child_key)
    os.makedirs(sub_folder, exist_ok=True)
    for raw_file, processed_file in zip(config['raw_dataset']['child_items'][child_key], child_values):
      src_file_path = os.path.join(config['raw_dataset']['dest_folder'],child_key, raw_file)
      dest_file_path = os.path.join(sub_folder, processed_file)
      transform_data(function_dict, src_file_path, dest_file_path )
  

if __name__=="__main__":
  transform_datasets()