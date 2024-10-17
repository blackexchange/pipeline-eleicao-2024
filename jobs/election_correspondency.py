import logging
import pandas as pd
import sys
import os
import shutil
import sys
from pyspark.sql import SparkSession


from data_transformations.election import ingest

LOG_FILENAME = 'project.log'
APP_NAME = "Election Pipeline: Ingest Corresponcency"

# Função para configurar o logging
def setup_logging():
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Função para baixar o arquivo
def download_and_unzip(params, input_path):
    param = params.split('_')
    #1t_BA_061020241406
    uf_list = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO','TO','ZZ']
    uf_list = ['AC','AL']
    for uf in uf_list:
        uf_filename = f"CESP_{param[0]}t_{uf}_{param[1]}.zip"
        url_download = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/correspesp/"
        zip_filepath = os.path.join(input_path, uf_filename)
        
        # Download do arquivo
        try:
            logging.info(f"Baixando o arquivo {uf_filename}...")
            ingest.download_file(url_download + uf_filename, input_path, uf_filename)
        except Exception as e:
            logging.error(f"Erro ao baixar o arquivo {uf_filename}: {e}")
            sys.exit(1)
 
        # Descompactar o arquivo
        try:
            logging.info(f"Descompactando o arquivo {uf_filename}...")
            ingest.unzip_file(zip_filepath, input_path + "/unzipped" ,'csv')
        except Exception as e:
            logging.error(f"Erro ao descompactar o arquivo {uf_filename}: {e}")
            sys.exit(1) 

# Função para processar os logs extraídos
def process_extracted_logs(input_path):
    logs_dir = input_path +  "/logs"
    unzipped_dir = input_path + "/unzipped"
    
    #logging.error(f"Extraindo logs {unzipped_dir} para {logs_dir}")
    
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Iterar sobre os arquivos descompactados
    for filename in os.listdir(unzipped_dir):
        file_path = unzipped_dir +"/"+ filename
        if os.path.isfile(file_path):
            try:
                ingest.extract_log_text(file_path, logs_dir)
            except Exception as e:
                logging.error(f"Erro ao extrair o arquivo {file_path}: {e}")


# Função para limpar arquivos temporários
def clean_up(input_path):
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, input_path)
    
    try:
        #shutil.rmtree(os.path.join(input_path, "unzipped"))
        #shutil.rmtree(os.path.join(input_path, "logs"))
        os.remove(file_path)

        logging.info("Arquivos temporários removidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao remover arquivos temporários: {e}")


def generate_parquet(input_path, output_path):

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
   
    ingest.run(spark, input_path, output_path)
    spark.stop()


def ler(input_csv_path):
    SPARK = SparkSession.builder.appName(APP_NAME).getOrCreate()
    actual = SPARK.read.parquet(input_csv_path)
    #actual.show(actual.count(), truncate=False)

   # actual.show(truncate=False)

    
# Função principal
def main(input_path, output_path, params):
    logging.info("Aplicação inicializada: " + APP_NAME)
    #ler("out/")
    download_and_unzip(params, input_path)
   # process_extracted_logs(input_path)
   # generate_parquet(input_path+"/logs", output_path)
    clean_up(input_path)
    
    logging.info("Aplicação finalizada: " + APP_NAME)

# Execução do script
if __name__ == '__main__':
    setup_logging()
    
    if len(sys.argv) != 4:
        logging.warning("Caminho de entrada, caminho de saída e UF são obrigatórios.")
        sys.exit(1)
   
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    uf = sys.argv[3]

    main(input_path, output_path, uf)
