from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator,
                               UploadToS3Operator)
import json
import math
import pandas as pd
import os
import logging
from helpers import SqlQueries


redshift_id = "redshift"
aws_id = "aws_credentials"
s3_id = "data_dest"

home = str(Path.home())
s3_bucket = "d-blue-final-project-bucket"
data_dir = home + "/Data"
output_dir = data_dir + "/Output"
stocks = "/Stocks/"
etfs = "/ETFs/"
homes = "/Homes/"
companies = "/Companies/"

input_stock_dir = data_dir + stocks
input_etf_dir = data_dir + etfs
input_home_dir = data_dir + homes
input_companies_dir = data_dir + companies

output_stock_dir = output_dir + stocks
output_etf_dir = output_dir + etfs
output_home_dir = output_dir + homes
output_company_dir = output_dir + companies

s3_stock_key = "Data" + stocks
s3_etf_key = "Data" + etfs
s3_home_key = "Data" + homes
s3_company_key = "Data" + companies

s3_stock_dir = "s3://"+s3_bucket+"/Data" + stocks
s3_etf_dir = "s3://"+s3_bucket+"/Data" + etfs
s3_home_dir = "s3://"+s3_bucket+"/Data"+ homes
s3_company_dir = "s3://"+s3_bucket+"/Data" + companies

company_file = "companies.csv"
sales_count = "Sale_Counts_State.json"
sales_price = "Sale_Prices_State.json"
listing_price = "State_MedianListingPrice_AllHomes.json"
listing_price_sqft = "State_MedianListingPricePerSqft_AllHomes.json"
values = "State_Zhvi_AllHomes.json"
values_sqft = "State_ZriPerSqft_AllHomes.json"


def safe_float(val, default=None):
    if val is None:
        return default
    elif math.isnan(val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def convert_to_json(file_name, in_dir, out_dir):
    df = pd.read_csv(in_dir + file_name)
    columns = list(df.columns)
    cleaned = [x for x in columns if x[0] == '2']
    states = df['RegionName']
    df.set_index('RegionName', inplace=True)
    with open(out_dir+file_name.split(".")[0] + ".json", 'w') as outfile:
        for x in range(len(cleaned)):
            for y in range(len(states)):
                json.dump({
                'state': states[y],
                'date': cleaned[x],
                'value': safe_float(df.loc[states[y], cleaned[x]], 0.0)
            }, outfile)

    
def prep_stock_etf_data(input_dir, out_dir):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    if len(os.listdir(out_dir)) > 0:
        return
    for filename in os.listdir(input_dir):
        with open(input_dir + filename, 'r') as f:
            if os.path.getsize(input_dir + filename) > 0:
                file_lines = [''.join(["Company_Abbr", ",", f.readline().strip(), '\n'])]
                tmp = [''.join([filename.split(".")[0], ",", x.strip(), '\n']) for x in f.readlines()]
                file_lines = file_lines + tmp
            else:
                continue

        with open(out_dir + filename.split(".")[0] + ".csv", 'w') as f:
            f.writelines(file_lines)


def prep_stock_data():
    prep_stock_etf_data(input_stock_dir, output_stock_dir)


def prep_etf_data():
    prep_stock_etf_data(input_etf_dir, output_etf_dir)


def prep_home_data():
    if not os.path.exists(output_home_dir):
        os.makedirs(output_home_dir)
    if len(os.listdir(output_home_dir)) > 0:
        return
    for filename in os.listdir(input_home_dir):
        if os.path.getsize(input_home_dir + filename) > 0:
            convert_to_json(filename, input_home_dir, output_home_dir)
        else:
            continue


def prep_company_data():
    if not os.path.exists(output_company_dir):
        os.makedirs(output_company_dir)
    if len(os.listdir(output_company_dir)) > 0:
        return
    with open(input_companies_dir + company_file, 'r', encoding="utf8") as f:
        if os.path.getsize(input_companies_dir + company_file) > 0:
            file_lines = [''.join([f.readline().strip().replace(" ", "_"), '\n'])]
            tmp = [''.join([x.strip().replace(", ","\, ").replace("'","\'"), '\n']) for x in f.readlines()]
            file_lines = file_lines + tmp
            with open(output_company_dir + company_file, 'w') as fw:
                fw.writelines(file_lines)


def verify_file_sizes(dir):
    for filename in os.listdir(dir):
        if os.path.getsize(dir + filename) > 0:
            logging.info(f"File {filename} in directory {dir} is not empty and is ready for staging.")
        else:
            raise ValueError(f"File {filename} in directory {dir} is empty and is not ready for staging.")


def verify_prep_data():
    verify_file_sizes(output_etf_dir)
    verify_file_sizes(output_stock_dir)
    verify_file_sizes(output_home_dir)
    verify_file_sizes(output_company_dir)


default_args = {
    'owner': 'D\'Juan Blue',
    'start_date': datetime(2019, 7, 22),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('stock_home_dag',
          default_args=default_args,
          description='Load and transform stock and home data in Redshift with Airflow',
          #schedule_interval='0 * * * *',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stock_prep_task = PythonOperator(
    task_id="prep_stock_data",
    dag=dag,
    python_callable=prep_stock_data
)

etf_prep_task = PythonOperator(
    task_id="prep_etf_data",
    dag=dag,
    python_callable=prep_etf_data
)

home_prep_task = PythonOperator(
    task_id="prep_home_data",
    dag=dag,
    python_callable=prep_home_data
)

company_prep_task = PythonOperator(
    task_id="prep_company_data",
    dag=dag,
    python_callable=prep_company_data
)

verify_prep_task = PythonOperator(
    task_id="verify_prep_data",
    dag=dag,
    python_callable=verify_prep_data
)

upload_stock_to_s3 = UploadToS3Operator(
    task_id="Upload_Stock_to_S3",
    dag=dag,
    s3_id=s3_id,
    bucket=s3_bucket,
    key=s3_stock_key,
    directory=output_stock_dir
)
upload_etf_to_s3 = UploadToS3Operator(
    task_id="Upload_ETF_to_S3",
    dag=dag,
    s3_id=s3_id,
    bucket=s3_bucket,
    key=s3_etf_key,
    directory=output_etf_dir
)

upload_company_to_s3 = UploadToS3Operator(
    task_id="Upload_Company_to_S3",
    dag=dag,
    s3_id=s3_id,
    bucket=s3_bucket,
    key=s3_company_key,
    directory=output_company_dir
)

upload_home_to_s3 = UploadToS3Operator(
    task_id="Upload_Home_to_S3",
    dag=dag,
    s3_id=s3_id,
    bucket=s3_bucket,
    key=s3_home_key,
    directory=output_home_dir
)

stage_stock_to_redshift = StageToRedshiftOperator(
    task_id='Stage_stock_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_stocks,
    create_params=SqlQueries.staging_stock_etf_create,
    s3_path=s3_stock_dir,
    is_json=False
)

stage_etf_to_redshift = StageToRedshiftOperator(
    task_id='Stage_etf_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_etfs,
    create_params=SqlQueries.staging_stock_etf_create,
    s3_path=s3_etf_dir,
    is_json=False
)

stage_company_to_redshift = StageToRedshiftOperator(
    task_id='Stage_company_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_companies,
    create_params=SqlQueries.staging_company_create,
    s3_path=s3_company_dir + company_file,
    is_json=False
)

stage_sales_price_to_redshift = StageToRedshiftOperator(
    task_id='Stage_home_sales_price_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_sales_price,
    create_params=SqlQueries.staging_price_create,
    s3_path=s3_home_dir + sales_price,
    is_json=True
)

stage_sales_count_to_redshift = StageToRedshiftOperator(
    task_id='Stage_home_sales_count_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_sales_count,
    create_params=SqlQueries.staging_count_value_create,
    s3_path=s3_home_dir + sales_count,
    is_json=True
)

stage_listing_price_to_redshift = StageToRedshiftOperator(
    task_id='Stage_listing_price_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_listing_price,
    create_params=SqlQueries.staging_price_create,
    s3_path=s3_home_dir + listing_price,
    is_json=True
)

stage_listing_price_sqft_to_redshift = StageToRedshiftOperator(
    task_id='Stage_listing_price_sqft_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_listing_price_sqft,
    create_params=SqlQueries.staging_sqft_create,
    s3_path=s3_home_dir + listing_price_sqft,
    is_json=True
)

stage_values_to_redshift = StageToRedshiftOperator(
    task_id='Stage_values_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_home_values,
    create_params=SqlQueries.staging_count_value_create,
    s3_path=s3_home_dir + values,
    is_json=True
)

stage_values_sqft_to_redshift = StageToRedshiftOperator(
    task_id='Stage_values_sqft_data',
    dag=dag,
    redshift_conn_id=redshift_id,
    aws_credentials_id=aws_id,
    staging_table=SqlQueries.staging_home_values_sqft,
    create_params=SqlQueries.staging_sqft_create,
    s3_path=s3_home_dir + values_sqft,
    is_json=True
)

run_staging_quality_checks = DataQualityOperator(
    task_id='Run_staging_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_id,
    tables=SqlQueries.staging_table_list
)

load_date_dim_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.date,
    dest_table_create_params=SqlQueries.date_create,
    dest_table_insert_values=SqlQueries.date_insert
)

load_state_dim_table = LoadDimensionOperator(
    task_id='Load_state_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.state,
    dest_table_create_params=SqlQueries.state_create,
    dest_table_insert_values=SqlQueries.state_insert
)

load_company_dim_table = LoadDimensionOperator(
    task_id='Load_company_dim_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.companies,
    dest_table_create_params=SqlQueries.company_create,
    dest_table_insert_values=SqlQueries.company_insert
)

run_dim_quality_checks = DataQualityOperator(
    task_id='Run_dim_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_id,
    tables=SqlQueries.dim_table_list
)

load_value_fact_table = LoadFactOperator(
    task_id='Load_value_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.home_value,
    dest_table_create_params=SqlQueries.home_value_create,
    dest_table_insert_values=SqlQueries.value_insert
)

load_value_sqft_fact_table = LoadFactOperator(
    task_id='Load_value_per_sqft_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.home_value_sqft,
    dest_table_create_params=SqlQueries.home_value_sqft_create,
    dest_table_insert_values=SqlQueries.value_sqft_insert
)

load_listing_fact_table = LoadFactOperator(
    task_id='Load_listing_price_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.home_listing,
    dest_table_create_params=SqlQueries.home_listing_create,
    dest_table_insert_values=SqlQueries.listing_price_insert
)

load_listing_sqft_fact_table = LoadFactOperator(
    task_id='Load_listing_price_per_sqft_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.home_listing_sqft,
    dest_table_create_params=SqlQueries.home_listing_sqft_create,
    dest_table_insert_values=SqlQueries.listing_price_sqft_insert
)

load_sales_price_fact_table = LoadFactOperator(
    task_id='Load_sales_price_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.home_sales_price,
    dest_table_create_params=SqlQueries.home_sales_price_create,
    dest_table_insert_values=SqlQueries.sales_price_insert,
)

load_sales_count_fact_table = LoadFactOperator(
    task_id='Load_sales_count_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.home_sales_count,
    dest_table_create_params=SqlQueries.home_sales_count_create,
    dest_table_insert_values=SqlQueries.sales_count_insert,
)

load_stock_open_fact_table = LoadFactOperator(
    task_id='Load_stock_open_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.stock_open,
    dest_table_create_params=SqlQueries.stock_open_create,
    dest_table_insert_values=SqlQueries.stock_open_insert,
)

load_stock_high_fact_table = LoadFactOperator(
    task_id='Load_stock_high_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.stock_high,
    dest_table_create_params=SqlQueries.stock_highs_create,
    dest_table_insert_values=SqlQueries.stock_high_insert,
)

load_stock_low_fact_table = LoadFactOperator(
    task_id='Load_stock_low_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.stock_low,
    dest_table_create_params=SqlQueries.stock_lows_create,
    dest_table_insert_values=SqlQueries.stock_low_insert,
)

load_stock_close_fact_table = LoadFactOperator(
    task_id='Load_stock_close_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.stock_close,
    dest_table_create_params=SqlQueries.stock_close_create,
    dest_table_insert_values=SqlQueries.stock_close_insert,
)

load_stock_volume_fact_table = LoadFactOperator(
    task_id='Load_stock_volume_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.stock_volume,
    dest_table_create_params=SqlQueries.stock_volume_create,
    dest_table_insert_values=SqlQueries.stock_volume_insert,
)

load_etf_open_fact_table = LoadFactOperator(
    task_id='Load_etf_open_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.etf_open,
    dest_table_create_params=SqlQueries.etf_open_create,
    dest_table_insert_values=SqlQueries.etf_open_insert,
)

load_etf_high_fact_table = LoadFactOperator(
    task_id='Load_etf_high_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.etf_high,
    dest_table_create_params=SqlQueries.etf_highs_create,
    dest_table_insert_values=SqlQueries.etf_high_insert,
)

load_etf_low_fact_table = LoadFactOperator(
    task_id='Load_etf_low_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.etf_low,
    dest_table_create_params=SqlQueries.etf_lows_create,
    dest_table_insert_values=SqlQueries.etf_low_insert,
)

load_etf_close_fact_table = LoadFactOperator(
    task_id='Load_etf_close_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.etf_close,
    dest_table_create_params=SqlQueries.etf_close_create,
    dest_table_insert_values=SqlQueries.etf_close_insert,
)

load_etf_volume_fact_table = LoadFactOperator(
    task_id='Load_etf_volume_fact_table',
    dag=dag,
    redshift_conn_id=redshift_id,
    dest_table_name=SqlQueries.etf_volume,
    dest_table_create_params=SqlQueries.etf_volume_create,
    dest_table_insert_values=SqlQueries.etf_volume_insert,
)

run_fact_quality_checks = DataQualityOperator(
    task_id='Run_fact_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_id,
    tables=SqlQueries.fact_table_list
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

#Prep Data
start_operator >> company_prep_task
start_operator >> stock_prep_task
start_operator >> home_prep_task
start_operator >> etf_prep_task
#Validate Prep
company_prep_task >> verify_prep_task
stock_prep_task >> verify_prep_task
etf_prep_task >> verify_prep_task
home_prep_task >> verify_prep_task
#Load to S3
verify_prep_task >> upload_company_to_s3
verify_prep_task >> upload_etf_to_s3
verify_prep_task >> upload_stock_to_s3
verify_prep_task >> upload_home_to_s3
#Stage to Redshift
upload_company_to_s3 >> stage_company_to_redshift
upload_stock_to_s3 >> stage_stock_to_redshift
upload_etf_to_s3 >> stage_etf_to_redshift
upload_home_to_s3 >> stage_listing_price_sqft_to_redshift
upload_home_to_s3 >> stage_listing_price_to_redshift
upload_home_to_s3 >> stage_sales_count_to_redshift
upload_home_to_s3 >> stage_sales_price_to_redshift
upload_home_to_s3 >> stage_values_sqft_to_redshift
upload_home_to_s3 >> stage_values_to_redshift
#Validate Stage
stage_company_to_redshift >> run_staging_quality_checks
stage_stock_to_redshift >> run_staging_quality_checks
stage_etf_to_redshift >> run_staging_quality_checks
stage_values_to_redshift >> run_staging_quality_checks
stage_values_sqft_to_redshift >> run_staging_quality_checks
stage_sales_price_to_redshift >> run_staging_quality_checks
stage_sales_count_to_redshift >> run_staging_quality_checks
stage_listing_price_to_redshift >> run_staging_quality_checks
stage_listing_price_sqft_to_redshift >> run_staging_quality_checks
#Load Dim Tables
run_staging_quality_checks >> load_company_dim_table
run_staging_quality_checks >> load_date_dim_table
run_staging_quality_checks >> load_state_dim_table
#Validate Dim Tables
load_state_dim_table >> run_dim_quality_checks
load_date_dim_table >> run_dim_quality_checks
load_company_dim_table >> run_dim_quality_checks
#Load Fact Tables
run_dim_quality_checks >> load_listing_fact_table
run_dim_quality_checks >> load_listing_sqft_fact_table
run_dim_quality_checks >> load_value_fact_table
run_dim_quality_checks >> load_value_sqft_fact_table
run_dim_quality_checks >> load_sales_count_fact_table
run_dim_quality_checks >> load_sales_price_fact_table
run_dim_quality_checks >> load_stock_open_fact_table
run_dim_quality_checks >> load_stock_close_fact_table
run_dim_quality_checks >> load_stock_high_fact_table
run_dim_quality_checks >> load_stock_low_fact_table
run_dim_quality_checks >> load_stock_volume_fact_table
run_dim_quality_checks >> load_etf_open_fact_table
run_dim_quality_checks >> load_etf_high_fact_table
run_dim_quality_checks >> load_etf_low_fact_table
run_dim_quality_checks >> load_etf_close_fact_table
run_dim_quality_checks >> load_etf_volume_fact_table
#Validate Fact Tables
load_listing_fact_table >> run_fact_quality_checks
load_listing_sqft_fact_table >> run_fact_quality_checks
load_value_fact_table >> run_fact_quality_checks
load_value_sqft_fact_table >> run_fact_quality_checks
load_sales_price_fact_table >> run_fact_quality_checks
load_sales_count_fact_table >> run_fact_quality_checks
load_stock_volume_fact_table >> run_fact_quality_checks
load_stock_low_fact_table >> run_fact_quality_checks
load_stock_high_fact_table >> run_fact_quality_checks
load_stock_close_fact_table >> run_fact_quality_checks
load_stock_open_fact_table >> run_fact_quality_checks
load_etf_volume_fact_table >> run_fact_quality_checks
load_etf_close_fact_table >> run_fact_quality_checks
load_etf_low_fact_table >> run_fact_quality_checks
load_etf_high_fact_table >> run_fact_quality_checks
load_etf_open_fact_table >> run_fact_quality_checks
#End
run_fact_quality_checks >> end_operator
