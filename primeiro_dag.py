from urllib import request
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url) # Capturando informaçoes da URL
    df = pd.DataFrame(json.loads(response.content)) # Transformando o arquivo JSON em um DataFrame
    qtd = len(df.index)
    return qtd

def e_valida(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados') # Recebendo as informaçoes da task captura_conta_dados
    if(qtd > 10): # verificando o numero de linhas
        return 'valido'
    return 'nvalido'      

# Alocando a Dag
with DAG('primeiro_dag', start_date = datetime(2022,3,14),
          schedule_interval = '30 * * * *', catchup= False) as dag:

    # Criando as Taks \/ 
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )      

    e_valida = BranchPythonOperator(
        task_id = 'e_valida',
        python_callable = e_valida
    )

    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )

    nvalido = BashOperator(
        task_id = 'nvalido',
        bash_command = "echo 'Quantidade não OK'"
    )

    # Definindo a ordem das tasks
    captura_conta_dados >> e_valida >> [valido, nvalido]
