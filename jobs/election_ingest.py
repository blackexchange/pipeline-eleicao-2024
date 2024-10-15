import logging
import pandas as pd
import sys
import os
import shutil
import sys
from pyspark.sql import SparkSession


from data_transformations.election import ingest

LOG_FILENAME = 'project.log'
APP_NAME = "Election Pipeline: Ingest"

# Função para configurar o logging
def setup_logging():
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Função para baixar o arquivo
def download_and_unzip(uf, input_path):
    uf_filename = f"bu_imgbu_logjez_rdv_vscmr_2024_1t_{uf}.zip"
    url_download = "https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/arqurnatot/"
    zip_filepath = os.path.join(input_path, f"{uf}.zip")
    
    # Download do arquivo
    try:
        logging.info(f"Baixando o arquivo {uf_filename}...")
        ingest.download_file(url_download + uf_filename, input_path, uf + ".zip")
    except Exception as e:
        logging.error(f"Erro ao baixar o arquivo {uf_filename}: {e}")
        sys.exit(1)

    # Descompactar o arquivo
    try:
        logging.info(f"Descompactando o arquivo {uf}.zip...")
        ingest.unzip_file(zip_filepath, os.path.join(input_path, "unzipped"))
    except Exception as e:
        logging.error(f"Erro ao descompactar o arquivo {uf}.zip: {e}")
        sys.exit(1)

# Função para processar os logs extraídos
def process_extracted_logs(input_path):
    logs_dir = os.path.join(input_path, "logs")
    unzipped_dir = os.path.join(input_path, "unzipped")
    
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    logging.info("Extraindo arquivos de log...")
    
    # Iterar sobre os arquivos descompactados
    for filename in os.listdir(unzipped_dir):
        file_path = os.path.join(unzipped_dir, filename)
        if os.path.isfile(file_path):
            try:
                logging.info(f"Extraindo arquivo: {file_path}")
                ingest.extract_log_text(file_path, logs_dir)
            except Exception as e:
                logging.error(f"Erro ao extrair o arquivo {file_path}: {e}")

# Função para gerar o DataFrame e salvar em CSV
# Função para gerar o DataFrame e salvar em CSV incrementalmente
def generate_output_csv(input_path, output_path, uf):
    logs_dir = os.path.join(input_path, "logs")
    csv_output = os.path.join(output_path, f"{uf}.csv")
    
    logging.info("Processando arquivos de log e gravando no CSV de forma incremental...")
    
    # Variável para controlar se o cabeçalho deve ser escrito ou não
    write_header = True if not os.path.exists(csv_output) else False
    
    # Itera sobre os arquivos no diretório e subdiretórios
    for root, dirs, files in os.walk(logs_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            try:
                logging.info(f"Processando arquivo: {file_path}")
                processed_df = ingest.process_log_file(file_path)
                
                # Escreve o DataFrame no arquivo CSV de forma incremental (append)
                processed_df.to_csv(csv_output, mode='a', header=write_header, index=False, encoding='latin-1')
                
                # Depois do primeiro arquivo, não escreva mais o cabeçalho
                write_header = False

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {file_path}: {e}")

    logging.info(f"Processamento completo e CSV gerado: {csv_output}")


# Função para limpar arquivos temporários
def clean_up(input_path):
    try:
        shutil.rmtree(os.path.join(input_path, "unzipped"))
        shutil.rmtree(os.path.join(input_path, "logs"))
        logging.info("Arquivos temporários removidos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao remover arquivos temporários: {e}")


def spar(input_path, output_path):

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    sc = spark.sparkContext
   
    ingest.run(spark, input_path, output_path)
    spark.stop()


def ler(input_csv_path):
    SPARK = SparkSession.builder.appName(APP_NAME).getOrCreate()
    actual = SPARK.read.parquet(input_csv_path)
    #actual.show(actual.count(), truncate=False)

   # actual.show(truncate=False)

    
# Função principal
def main(input_path, output_path, uf):
    logging.info("Aplicação inicializada: " + APP_NAME)
    #spar(input_path, output_path)
    ler("out/")
    #download_and_unzip(uf, input_path)
    #process_extracted_logs(input_path)
    #generate_output_csv(input_path, output_path, uf)
   # clean_up(input_path)
    
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
