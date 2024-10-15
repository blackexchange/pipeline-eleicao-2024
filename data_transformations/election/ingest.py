import pandas as pd
from typing import List
from bs4 import BeautifulSoup
import requests
import os
import zipfile
import shutil

import logging
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date, lit, to_timestamp, lag, lead, when
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def list_totalization_files(url="https://dadosabertos.tse.jus.br/dataset/resultados-2024-arquivos-transmitidos-para-totalizacao"):

    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")

    ul = soup.find('ul', class_='resource-list')
    lis = ul.find_all("li")

    ds_files = []

    for li in lis:

        link = li.find('a', class_='heading')
        if link:
            title = link['title']

            linkCdn = li.find('a', class_='resource-url-analytics')

            if linkCdn:
                linkFile = linkCdn['href']


            if linkFile.endswith('zip'):
                file_name = linkFile.split("/")[-1]
                ds_files.append(file_name)
    return ds_files


def download_file(linkFile,path,file_name):
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, path,file_name)
    

    if os.path.exists(file_path):
        return True
        
    print(f"Baixando { file_name}...")

    response = requests.get(linkFile, stream=True)

   
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):  # Definido para baixar em pedaços de 8KB
            f.write(chunk)
                
    if(os.path.exists(file_path)):
        return True
    else:
        return False



def unzip_file(zip_path, extract_to):
    print(f"Extraindo arquivos...")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Verifica se a extensão do arquivo corresponde ao filtro
            if file_info.filename.endswith("jez"):
                # Extrai apenas os arquivos que correspondem ao filtro de extensão
                zip_ref.extract(file_info, extract_to)

    
    if(os.path.exists(extract_to)):
        return True
    else:
        return False
    



def extract_log_text(zip_path, extract_to):
    print(f"Extraindo logs...")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        dir_name = os.path.basename(zip_path)
        dir_name = str(dir_name).split('.')[0]
        for file_info in zip_ref.infolist():
            if file_info.filename.endswith(".dat"):
                # Extrai apenas os arquivos que correspondem ao filtro de extensão
                zip_ref.extract(f"{extract_to}/{dir_name}")

    if(os.path.exists(extract_to)):
        return True
    else:
        return False


def run(spark: SparkSession, ingest_path: str, output_path: str) -> int:
    print(f"Gerando DataFrame no Spark...")

    schema = StructType([
        StructField("DataHora", StringType(), True),
        StructField("LogLevel", StringType(), True),
        StructField("CodigoUrna", StringType(), True),
        StructField("Acao", StringType(), True),
        StructField("Mensagem", StringType(), True),
        StructField("Código Verificação", StringType(), True)
    ])

    df = spark.read.csv(ingest_path + "/**/*.dat", sep='\t', header=False, schema=schema,  encoding='latin1')

    df = df.withColumn("DataHora", to_timestamp(df["DataHora"], "dd/MM/yyyy HH:mm:ss"))

    # Filtrar os eventos que correspondem a "Eleitor foi habilitado" e "O voto do eleitor foi computado"
    df_filtered = df.filter(
        (F.col("Acao") =="VOTA") & 
        (F.col("DataHora") >= "2024-10-06") &
        ((F.col("Mensagem").like("%Eleitor foi habilitado%")) | 
        (F.col("Mensagem").like("%O voto do eleitor foi computado%")))
    )

    # Usar windowing para identificar o próximo evento "O voto do eleitor foi computado"
    window = Window.partitionBy("CodigoUrna").orderBy("DataHora")

    # Encontrar o próximo evento correspondente
    df_result = df_filtered.withColumn("Next_Event", F.lead("Mensagem").over(window)) \
        .withColumn("Next_DataHora", F.lead("DataHora").over(window)) \
        .filter((F.col("Mensagem").like("%Eleitor foi habilitado%")) & 
                (F.col("Next_Event").like("%O voto do eleitor foi computado%")))

    # Selecionar as colunas necessárias
    df_final = df_result.select(
        "CodigoUrna", 
        "DataHora", 
        F.col("Next_DataHora").alias("DataHora_Fim"),
        (F.unix_timestamp("Next_DataHora") - F.unix_timestamp("DataHora")).alias("TempoSec")  # Diferença em segundos

    )
    df_final.write.mode("overwrite").parquet(output_path)
    df_final.show(5, truncate=False)
    return df_final.count()

    

def process_log_file(file_path):
    print(f"Gerando dataframe...")
    
    # Lê o arquivo CSV delimitado por tabulações
    df = pd.read_csv(file_path, 
                     sep='\t', 
                     header=None, 
                     names=['Data e Hora', 'Log Level', 'Código Urna', 'Ação', 'Mensagem', 'Código Verificação'],
                     encoding='latin-1',  # Mudando a codificação para 'latin-1'
                     on_bad_lines='skip')

    # Adiciona o nome do arquivo como uma nova coluna
    df['Arquivo'] = file_path

    # Divide a coluna 'Data e Hora' em 'Data' e 'Hora'
    df[['Data', 'Hora']] = df['Data e Hora'].str.split(' ', n=1, expand=True)

    # Converte a coluna 'Data' para formato datetime
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y')

    # Filtra as datas superiores ou iguais a 06/10/2024
    df = df[df['Data'] >= '2024-10-06']

    # Inicializar variáveis para armazenar as informações da urna
    urna_info = {
        'Turno da UE': None,
        'Modelo de Urna': None,
        'Fase da UE': None,
        'Modo de Carga da UE': None,
        'Município': None,
        'Zona Eleitoral': None,
        'Seção Eleitoral': None
    }

    # Varre o DataFrame linha por linha para preencher as informações da urna
    for index, row in df.iterrows():
        if pd.isna(urna_info['Turno da UE']) and 'Turno da UE' in row['Mensagem']:
            urna_info['Turno da UE'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Modelo de Urna']) and 'Modelo de Urna' in row['Mensagem']:
            urna_info['Modelo de Urna'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Fase da UE']) and 'Fase da UE' in row['Mensagem']:
            urna_info['Fase da UE'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Modo de Carga da UE']) and 'Modo de carga da UE' in row['Mensagem']:
            urna_info['Modo de Carga da UE'] = row['Mensagem'].split('-')[-1].strip()

        if pd.isna(urna_info['Município']) and 'Município' in row['Mensagem']:
            urna_info['Município'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Zona Eleitoral']) and 'Zona Eleitoral' in row['Mensagem']:
            urna_info['Zona Eleitoral'] = row['Mensagem'].split(':')[-1].strip()

        if pd.isna(urna_info['Seção Eleitoral']) and 'Seção Eleitoral' in row['Mensagem']:
            urna_info['Seção Eleitoral'] = row['Mensagem'].split(':')[-1].strip()

        # Agora que temos todas as informações da urna, podemos preenchê-las nas linhas seguintes
        if all(v is not None for v in urna_info.values()):
            break

    # Agora vamos aplicar essas informações para todas as linhas
    for col, value in urna_info.items():
        df[col] = value

    # Filtra apenas as linhas com log level 'INFO' e ação 'VOTA'
    df_votos = df[(df['Log Level'] == 'INFO') & (df['Ação'] == 'VOTA')]

    # Identificação de início e fim do voto
    start_vote_mask = df_votos['Mensagem'].str.contains('Eleitor foi habilitado')
    end_vote_mask = df_votos['Mensagem'].str.contains('O voto do eleitor foi computado')

    # Cria DataFrames separados para início e fim do voto
    df_start = df_votos[start_vote_mask].copy()
    df_end = df_votos[end_vote_mask].copy()

    # Adiciona uma coluna de índice para rastrear a ordem de entrada no log
    df_start['Voto ID'] = range(1, len(df_start) + 1)
    df_end['Voto ID'] = range(1, len(df_end) + 1)

    # Renomeia as colunas para distinguir início e fim do voto
    df_start.rename(columns={'Hora': 'Hora Início', 'Mensagem': 'Mensagem Início'}, inplace=True)
    df_end.rename(columns={'Hora': 'Hora Fim', 'Mensagem': 'Mensagem Fim'}, inplace=True)

    # Merge (juntar) os dados de início e fim baseados no índice Voto ID
    df_combined = pd.merge(df_start[['Voto ID', 'Data', 'Hora Início', 'Mensagem Início']],
                           df_end[['Voto ID', 'Data', 'Hora Fim', 'Mensagem Fim']],
                           on=['Voto ID', 'Data'], how='inner')

    # Adiciona as informações da urna ao DataFrame combinado
    for col, value in urna_info.items():
        df_combined[col] = value

    # Exibe o DataFrame combinado (opcional)
    return df_combined

def remove_files(directory_path):
    if os.path.exists(directory_path):
        # Remove o diretório e todo o conteúdo dentro dele
        shutil.rmtree(directory_path)
        print(f"Diretório '{directory_path}' removido com sucesso.")
    else:
        print(f"O diretório '{directory_path}' não existe.")

