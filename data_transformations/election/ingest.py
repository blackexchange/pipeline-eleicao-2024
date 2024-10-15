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


def run(spark: SparkSession, ingest_path: str, output_path: str) -> None:
    print(f"Gerando DataFrame no Spark...")

    schema = StructType([
        StructField("Data e Hora", StringType(), True),
        StructField("Log Level", StringType(), True),
        StructField("Código Urna", StringType(), True),
        StructField("Ação", StringType(), True),
        StructField("Mensagem", StringType(), True),
        StructField("Código Verificação", StringType(), True)
    ])

    ingest_path = ingest_path + "/**/*.dat"  # ** procura em todas as subpastas

    # Lê os arquivos .dat delimitados por tabulações no Spark
    df = spark.read.csv(ingest_path, sep='\t', header=False, schema=schema,  encoding='latin1')

    # Verificar o esquema inicial e exibir algumas linhas
    df.printSchema()
    df.show(5, truncate=False)
     
    # Verificar o número de colunas lidas
    if len(df.columns) == 6:  # O número esperado de colunas
        # Renomeia as colunas
        df = df.toDF('Data e Hora', 'Log Level', 'Código Urna', 'Ação', 'Mensagem', 'Código Verificação')
    else:
        raise ValueError(f"O número de colunas lidas ({len(df.columns)}) não corresponde ao esperado (6).")

    # O restante do processamento continua igual...
    # Adiciona o nome do arquivo como uma nova coluna
    #df = df.withColumn('Arquivo', lit(ingest_path))
    

    # Divide a coluna 'Data e Hora' em 'Data' e 'Hora'
    #df = df.withColumn('Data', split(df['Data e Hora'], ' ').getItem(0)) \
     #      .withColumn('Hora', split(df['Data e Hora'], ' ').getItem(1))

    # Converte a coluna 'Data' para o formato datetime
    #df = df.withColumn('Data', to_date(df['Data'], 'dd/MM/yyyy'))

    df = df.withColumn("DataHora", to_timestamp(df["Data e Hora"], "dd/MM/yyyy HH:mm:ss"))


    # Filtra as datas superiores ou iguais a 06/10/2024
    df = df.filter(df['DataHora'] >= '2024-10-06')
    df.show(5, truncate=False)

    # Inicializar variáveis para armazenar as informações da urna
    urna_info = {
        'Turno da UE': None,
        'Modelo de Urna': None,
        'Município': None,
        'Zona Eleitoral': None,
        'Seção Eleitoral': None
    }


    # Filtramos as mensagens que contém as informações da urna e as coletamos para preenchê-las
    urna_info_patterns = {
        'Turno da UE': 'Turno da UE',
        'Modelo de Urna': 'Modelo de Urna',
        'Município': 'Município',
        'Zona Eleitoral': 'Zona Eleitoral',
        'Seção Eleitoral': 'Seção Eleitoral'
    }

    for info_key, pattern in urna_info_patterns.items():
        row = df.filter(df['Mensagem'].contains(pattern)).select('Mensagem').first()
        if row:
            urna_info[info_key] = row['Mensagem'].split(':')[-1].strip()

    # Adicionar as informações da urna como colunas no DataFrame
    for col_name, value in urna_info.items():
        df = df.withColumn(col_name, lit(value))

    # Filtra apenas as linhas com log level 'INFO' e ação 'VOTA'
    df_votos = df.filter((df['Log Level'] == 'INFO') & (df['Ação'] == 'VOTA'))
    df_votos = df_votos.drop('Log Level', 'Código Verificação', 'Ação', 'Código Urna','Data e Hora')

   # window = Window.orderBy("DataHora")

    # Ajuste: usa-se when() corretamente para definir a coluna "Início Voto"
   # df_votos = df_votos.withColumn("Início Voto", when(col("Mensagem").contains("Eleitor foi habilitado"), lag("DataHora").over(window)))

    # Ajuste: para "Fim Voto" (mesma lógica, usando lead)
    #df_votos = df_votos.withColumn("Fim Voto", when(col("Mensagem").contains("O voto do eleitor foi computado"), lead("DataHora").over(window)))


    # Identificar as linhas de início e fim de votação
    #df_start = df_votos.filter(df_votos['Mensagem'].contains('Eleitor foi habilitado'))
    #df_end = df_votos.filter(df_votos['Mensagem'].contains('O voto do eleitor foi computado'))

    # Adicionar um ID para cada voto
    #df_start = df_start.withColumn("Voto ID", F.row_number().over(Window.orderBy("Data", "Hora")))
    #df_end = df_end.withColumn("Voto ID", F.row_number().over(Window.orderBy("Data", "Hora")))

    # Juntar as informações de início e fim com base no "Voto ID"
    """ df_combined = df_start.join(df_end, on="Voto ID", how="inner") \
                          .select(df_start['Voto ID'], 
                                  df_start['Data'], 
                                  df_start['Hora'].alias('Hora Início'), 
                                  df_start['Mensagem'].alias('Mensagem Início'),
                                  df_end['Hora'].alias('Hora Fim'), 
                                  df_end['Mensagem'].alias('Mensagem Fim'))

    # Adiciona as informações da urna ao DataFrame combinado
    for col_name, value in urna_info.items():
        df_combined = df_combined.withColumn(col_name, lit(value))

    # Grava o DataFrame resultante no formato Parquet, adicionando ao diretório existente
    df_combined.write.mode('append').parquet(output_path) """
    df_votos.write.mode('append').parquet(output_path)

 
    print(f"Dados gravados no formato Parquet em: {output_path}")
    df_votos.show(5, truncate=False)
    

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

