import logging

import sys


from data_transformations.election import ingest

LOG_FILENAME = 'project.log'
APP_NAME = "Election Pipeline: Ingest"

if __name__ == '__main__':

    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(sys.argv)

    if len(sys.argv) != 3:
        logging.warning("Input source and output path are required")
        sys.exit(1)
   
    logging.info("Application Initialized: " + APP_NAME)
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    ingest.download_file("https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2024/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2024_1t_TO.zip",'in','bu_imgbu_logjez_rdv_vscmr_2024_1t_TO.zip')
    logging.info("Application Done: " + APP_NAME)

