airflow variables set load_type "incremental"
airflow variables set data_url "https://download.cms.gov/nppes/NPPES_Data_Dissemination_011325_011925_Weekly.zip"
airflow variables set time_period "20250113-20250119"

airflow dags trigger npi_reference_table_etl 

