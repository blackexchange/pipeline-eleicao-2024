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

# Função para baixar o arquivo
def prepare_files(params, input_path):
    param = params.split('_')
    #1t_BA_061020241406
    uf_list = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO','TO','ZZ']
    uf_list = ['AC','AL','SP']
    for uf in uf_list:
        uf_filename = f"CESP_{param[0]}t_{uf}_{param[1]}.zip"
        url_download = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/correspesp/"
        zip_filepath = os.path.join(input_path, uf_filename)
        
        # Download do arquivo
        try:
            logging.info(f"Baixando o arquivo {uf_filename}...")
            download_file(url_download + uf_filename, input_path, uf_filename)
        except Exception as e:
            logging.error(f"Erro ao baixar o arquivo {uf_filename}: {e}")
            
 
        # Descompactar o arquivo
        try:
            logging.info(f"Descompactando o arquivo {uf_filename}...")
            unzip_file(zip_filepath, input_path + "/unzipped" ,'csv')
        except Exception as e:
            logging.error(f"Erro ao descompactar o arquivo {uf_filename}: {e}")
             



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



def unzip_file(zip_path, extract_to, filter='jez'):
    print(f"Extraindo arquivos...")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Verifica se a extensão do arquivo corresponde ao filtro
            if file_info.filename.endswith(filter):
                # Extrai apenas os arquivos que correspondem ao filtro de extensão
                zip_ref.extract(file_info, extract_to)

    
    if(os.path.exists(extract_to)):
        return True
    else:
        return False
    


def run(spark: SparkSession, ingest_path: str, output_path: str) -> int:
    print(f"Gerando DataFrame no Spark...")

    df = spark.read.csv(ingest_path, sep=';', header=True, inferSchema=True,  encoding='latin1')

    # Filtrar os eventos que correspondem a "Eleitor foi habilitado" e "O voto do eleitor foi computado"
    df_filtered = df.select("AA_ELEICAO", "CD_PLEITO", "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO", "NR_ZONA", "NR_URNA_ESPERADA")

    df_final = df_filtered.dropDuplicates(["AA_ELEICAO", "CD_PLEITO", "SG_UF", "CD_MUNICIPIO", "NM_MUNICIPIO", "NR_ZONA", "NR_URNA_ESPERADA"])

    df_final.repartition(1).write.mode("overwrite").partitionBy("SG_UF").option("header", "true").csv(output_path + "/csv")
    df_final.repartition(1).write.mode("overwrite").partitionBy("SG_UF").option("header", "true").parquet(output_path + "/parquet")

    return df_final.count()


# Função para limpar arquivos temporários
def clean_up(input_path):
#    current_directory = os.getcwd()
    
    try:
        #shutil.rmtree(os.path.join(input_path, "unzipped"))
        shutil.rmtree(input_path)
        #os.remove(file_path)

        logging.info("Arquivos temporários removidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao remover arquivos temporários: {e}")

    if not os.path.exists(input_path):
        os.makedirs(input_path)
