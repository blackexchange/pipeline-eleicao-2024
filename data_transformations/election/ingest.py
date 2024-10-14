import pandas as pd
import re
import logging
from typing import List
from bs4 import BeautifulSoup
import requests
import os
import zipfile

def list_totalization_files():

    url = "https://dadosabertos.tse.jus.br/dataset/resultados-2024-arquivos-transmitidos-para-totalizacao"

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
        
    print("Baixando " + file_name)

    response = requests.get(linkFile, stream=True)

    print("Gravando " + file_name)
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):  # Definido para baixar em pedaços de 8KB
            f.write(chunk)
                
    if(os.path.exists(file_path)):
        return True
    else:
        return False



def unzip_file(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Verifica se a extensão do arquivo corresponde ao filtro
            if file_info.filename.endswith("jez"):
                # Extrai apenas os arquivos que correspondem ao filtro de extensão
                zip_ref.extract(file_info, extract_to)
                print(f"Extraído: {file_info.filename}")


    if(os.path.exists(extract_to)):
        return True
    else:
        return False
    


def extract_log_text(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Arquivos descompactados em: {extract_to}")


    if(os.path.exists(extract_to)):
        return True
    else:
        return False



def process_log_file(file_path):
    df = pd.read_csv(file_path, 
                     sep='\t', 
                     header=None, 
                     names=['Data e Hora', 'Log Level', 'Código Urna', 'Ação', 'Mensagem', 'Código Verificação'],
                     encoding="latin-1", 
                     on_bad_lines='skip')
    # Adiciona o nome do arquivo como uma nova coluna
    
    df['Arquivo'] = file_path

    # Divide a coluna 'Data e Hora' em 'Data' e 'Hora'
    df[['Data', 'Hora']] = df['Data e Hora'].str.split(' ', 1, expand=True)

    # Converte a coluna 'Data' para formato datetime
    df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y')

    # Filtra as datas superiores a 06/10/2024
    df = df[df['Data'] > '2024-10-06']

    # Filtra apenas as linhas com log level 'INFO' e ação 'VOTA'
    df = df[(df['Log Level'] == 'INFO') & (df['Ação'] == 'VOTA')]

    # Filtra mensagens de início de voto (Eleitor foi habilitado) e fim de voto (O voto foi computado)
    start_vote_mask = df['Mensagem'].str.contains('Eleitor foi habilitado')
    end_vote_mask = df['Mensagem'].str.contains('O voto do eleitor foi computado')

    # Cria DataFrames separados para início e fim do voto
    df_start = df[start_vote_mask].copy()
    df_end = df[end_vote_mask].copy()

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

    # Exibe o DataFrame combinado (opcional)
    print(df_combined)
    df_combined.to_csv()
    

    return True
