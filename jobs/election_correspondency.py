import logging
import sys
from pyspark.sql import SparkSession


from data_transformations.election_correspondency import ingest

LOG_FILENAME = 'project.log'
APP_NAME = "Election Pipeline: Ingest Corresponcency"

# Função para configurar o logging
def setup_logging():
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


def generate_parquet(input_path, output_path):

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
   
    ingest.run(spark, input_path + "/unzipped/*.csv", output_path)
    spark.stop()

# Função principal
def main(input_path, output_path, params):
    logging.info("Aplicação inicializada: " + APP_NAME)
    logging.info("Parametros: " + params)

    
    ingest.prepare_files(params, input_path)
    
    generate_parquet(input_path,output_path)

    ingest.clean_up(input_path)
    
    logging.info("Aplicação finalizada: " + APP_NAME)

# Execução do script
if __name__ == '__main__':
    setup_logging()
    
    if len(sys.argv) != 4:
        logging.warning("Caminho de entrada, caminho de saída e Parâmetros são obrigatórios.")
        sys.exit(1)
   
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    uf = sys.argv[3]

    main(input_path, output_path, uf)

