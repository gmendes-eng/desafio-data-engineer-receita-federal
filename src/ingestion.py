# src/ingestion.py (VERSÃO FINAL COM LANDING ZONE)

import requests
import zipfile
import logging
from pathlib import Path

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL base com a pasta de data
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"

URLS = [
    f"{BASE_URL}Empresas0.zip",
    f"{BASE_URL}Socios0.zip"
]

# Define os caminhos para a nova estrutura da "Landing Zone"
ARCHIVES_PATH = Path("data/bronze/landing/archives")
UNZIPPED_PATH = Path("data/bronze/landing/unzipped_csvs")

def executar_ingestao():
    """
    BRONZE (LANDING): Baixa os arquivos .zip da fonte e os descompacta
    em uma área de "pouso", mantendo o dado original (zip) e o descompactado (csv).
    """
    logging.info("Iniciando a etapa de ingestão (Bronze Landing Zone)...")
    
    ARCHIVES_PATH.mkdir(parents=True, exist_ok=True)
    UNZIPPED_PATH.mkdir(parents=True, exist_ok=True)
    
    for url in URLS:
        zip_filename = url.split("/")[-1]
        zip_filepath = ARCHIVES_PATH / zip_filename
        
        try:
            logging.info(f"Baixando o arquivo: {zip_filename}...")
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
            
            with open(zip_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Download de {zip_filename} concluído e salvo em {ARCHIVES_PATH}.")
            
            logging.info(f"Descompactando {zip_filename} para {UNZIPPED_PATH}...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(UNZIPPED_PATH)
            logging.info(f"Arquivo {zip_filename} descompactado com sucesso.")

        except Exception as e:
            logging.error(f"FALHA CRÍTICA na ingestão do arquivo {zip_filename}: {e}")
            raise e
            
    logging.info("Etapa de ingestão (Bronze Landing Zone) finalizada com sucesso.")