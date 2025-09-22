# src/ingestion.py

import requests
import zipfile
from pathlib import Path

# URL base com a pasta de data
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"

URLS = [
    f"{BASE_URL}Empresas0.zip",
    f"{BASE_URL}Socios0.zip"
]

# Define os caminhos para as novas subpastas da camada bronze
ARCHIVES_PATH = Path("data/bronze/archives")
RAW_FILES_PATH = Path("data/bronze/raw_files")

def executar_ingestao():
    """
    Realiza o download e a descompactação dos dados da Receita Federal
    para a camada Bronze, separando os arquivos .zip dos dados brutos.
    """
    print("Iniciando a etapa de ingestão (Camada Bronze Refinada)...")
    
    # Cria os diretórios da camada bronze, se não existirem
    ARCHIVES_PATH.mkdir(parents=True, exist_ok=True)
    RAW_FILES_PATH.mkdir(parents=True, exist_ok=True)
    
    for url in URLS:
        zip_filename = url.split("/")[-1]
        # O arquivo .zip será salvo na pasta 'archives'
        zip_filepath = ARCHIVES_PATH / zip_filename
        
        try:
            # 1. Download do arquivo
            print(f"Baixando o arquivo: {zip_filename}...")
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
            
            with open(zip_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Download de {zip_filename} concluído e salvo em {ARCHIVES_PATH}.")
            
            # 2. Descompactação do arquivo para a pasta 'raw_files'
            print(f"Descompactando {zip_filename} para {RAW_FILES_PATH}...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                # Extrai os arquivos para o diretório de arquivos brutos
                zip_ref.extractall(RAW_FILES_PATH)
            print(f"Arquivo {zip_filename} descompactado com sucesso.")

        except requests.exceptions.RequestException as e:
            print(f"Erro no download de {url}: {e}")
        except zipfile.BadZipFile:
            print(f"Erro: O arquivo {zip_filename} não é um ZIP válido ou está corrompido.")
        except Exception as e:
            print(f"Ocorreu um erro inesperado no processamento de {zip_filename}: {e}")
            
    print("Etapa de ingestão finalizada.")