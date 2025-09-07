from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import pandas as pd
import os
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Função para criar diretório se não existir
def create_directory(date):
    directory = f"data/{date.year}-{date.month:02d}-{date.day:02d}"
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    return directory

# Função para extrair dados do CSV
def extract_csv(**context):
    execution_date = context['execution_date']
    directory = create_directory(execution_date)
    
    # Leitura do arquivo CSV de transações
    df = pd.read_csv('/opt/airflow/csv_file/transacoes.csv') 
    
    # Salvando no formato especificado
    output_path = f"{directory}/csv/transacoes.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    
    return output_path

# Função para extrair dados do SQL
def extract_sql(**context):
    execution_date = context['execution_date']
    directory = create_directory(execution_date)
    
    # Conexão com o banco de dados
    conn = psycopg2.connect(
        dbname="banvic",
        user="data_engineer",
        password="v3rysecur&pas5w0rd",
        host="db",
        port="5432"
    )
    
    # Lista de tabelas para extrair
    tables = ['agencias', 'clientes', 'colaborador_agencia', 'colaboradores', 
              'contas', 'propostas_credito']
    
    extracted_files = []
    for table in tables:
        # Leitura da tabela
        query = f"SELECT * FROM {table}"
        df = pd.read_sql_query(query, conn)
        
        # Salvando no formato especificado
        output_path = f"{directory}/sql/{table}.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        extracted_files.append(output_path)
    
    conn.close()
    return extracted_files

# Função para carregar dados no Data Warehouse
def load_to_dw(**context):
    execution_date = context['execution_date']
    directory = create_directory(execution_date)
    
    # Configuração da conexão com o Data Warehouse
    dw_engine = create_engine('postgresql://data_engineer:v3rysecur&pas5w0rd@db:5432/banvic_dw')
    
    # Carregando dados do CSV
    csv_path = f"{directory}/csv/transacoes.csv"
    if os.path.exists(csv_path):
        df_transacoes = pd.read_csv(csv_path)
        df_transacoes.to_sql('transacoes_dw', dw_engine, if_exists='replace', index=False)
    
    # Carregando dados do SQL
    sql_tables = ['agencias', 'clientes', 'colaborador_agencia', 'colaboradores', 
                 'contas', 'propostas_credito']
    
    for table in sql_tables:
        sql_path = f"{directory}/sql/{table}.csv"
        if os.path.exists(sql_path):
            df = pd.read_csv(sql_path)
            df.to_sql(f"{table}_dw", dw_engine, if_exists='replace', index=False)

# Criação da DAG
with DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para extração de dados CSV e SQL',
    schedule_interval='35 4 * * *',  # Executa todos os dias às 04:35
    catchup=False
) as dag:
    
    # Tarefa inicial
    start = DummyOperator(task_id='start')
    
    # Tarefas de extração (executadas em paralelo)
    extract_csv_task = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv,
        provide_context=True
    )
    
    extract_sql_task = PythonOperator(
        task_id='extract_sql',
        python_callable=extract_sql,
        provide_context=True
    )
    
    # Tarefa de carregamento no DW
    load_dw_task = PythonOperator(
        task_id='load_to_dw',
        python_callable=load_to_dw,
        provide_context=True
    )
    
    # Tarefa final
    end = DummyOperator(task_id='end')
    
    # Definição do fluxo de tarefas
    start >> [extract_csv_task, extract_sql_task] >> load_dw_task >> end