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
    'start_date': datetime(2025, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Função para criar a pasta com as datas de extração
def criacao_pasta_data(date):
    pasta = f"data/{date.year}-{date.month:02d}-{date.day:02d}"
    if not os.path.exists(pasta):
        os.makedirs(pasta, exist_ok=True)
    return pasta

# Função para extrair dados do CSV vai receber informacoes do airflow como a data de execução
# Extracao do CSV com o formato especificado
def extract_csv(**context):
    data_execucao = context['execution_date']
    pasta = criacao_pasta_data(data_execucao)
    # Leitura do arquivo CSV de transações no Pandas
    df = pd.read_csv('/opt/airflow/csv_file/transacoes.csv') 
    # Salvando no formato especificado com as datas 
    saida_path = f"{pasta}/csv/transacoes.csv"
    os.makedirs(os.path.dirname(saida_path), exist_ok=True)
    df.to_csv(saida_path, index=False)
    
    return saida_path

# Função para extrair dados do SQL
def extract_sql(**context):
    data_execucao = context['execution_date']
    pasta = criacao_pasta_data(data_execucao)
    
    # Conexão com o banco de dados utilizando o psycopg2
    #para trabalhar com postgrees
    conexao_banco = psycopg2.connect(
        dbname="banvic",
        user="data_engineer",
        password="v3rysecur&pas5w0rd",
        host="db",
        port="5432"
    )
    
    # Lista de tabelas para extrair
    tabelas_banco = ['agencias', 'clientes', 'colaborador_agencia', 'colaboradores', 
              'contas', 'propostas_credito']
    
    extracao_arquivos = []
    for tabela_postgrees in tabelas_banco:
        # Leitura das tabelas do banco
        query = f"SELECT * FROM {tabela_postgrees}"
        df_postgress = pd.read_sql_query(query, conexao_banco)
        
        # Salvando no formato especificado
        pasta_path = f"{pasta}/sql/{tabela_postgrees}.csv"
        os.makedirs(os.path.dirname(pasta_path), exist_ok=True)
        df_postgress.to_csv(pasta_path, index=False)
        extracao_arquivos.append(pasta_path)
    
    conexao_banco.close()
    return extracao_arquivos

# Função para carregar dados no Data Warehouse
def load_to_dw(**context):
    data_execucao = context['execution_date']
    pasta = criacao_pasta_data(data_execucao)
    
    # Configuração da conexão com o Data Warehouse
    dw_engine = create_engine('postgresql://data_engineer:v3rysecur&pas5w0rd@db:5432/banvic_dw')
    directory = pasta
    # Carregando dados do CSV
    csv_path = f"{pasta}/csv/transacoes.csv"
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