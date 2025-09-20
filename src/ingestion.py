# src/ingestion.py

import requests
import zipfile
from pathlib import Path

# URLs diretas para os arquivos ZIP. Para o desafio, usamos apenas um de cada.
# Em um cenário real, poderíamos ter uma lista de todos os arquivos.
URLS = [
    "https://dadosabertos.rfb.gov.br/CNPJ/Empresas0.zip",
    "https://dadosabertos.rfb.gov.br/CNPJ/Socios0.zip"
]

# Define o caminho para a camada bronze
BRONZE_PATH = Path("data/bronze")

def executar_ingestao():
    """
    Realiza o download e a descompactação dos dados da Receita Federal
    para a camada Bronze.
    """
    print("Iniciando a etapa de ingestão (Camada Bronze)...")
    
    # Cria o diretório da camada bronze, se não existir
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    
    for url in URLS:
        # Extrai o nome do arquivo da URL
        zip_filename = url.split("/")[-1]
        zip_filepath = BRONZE_PATH / zip_filename
        
        try:
            # 1. Download do arquivo
            print(f"Baixando o arquivo: {zip_filename}...")
            response = requests.get(url, stream=True)
            # Lança uma exceção se o status code não for de sucesso (2xx)
            response.raise_for_status()
            
            with open(zip_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Download de {zip_filename} concluído.")
            
            # 2. Descompactação do arquivo
            print(f"Descompactando {zip_filename}...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(BRONZE_PATH)
            print(f"Arquivo {zip_filename} descompactado com sucesso.")

        except requests.exceptions.RequestException as e:
            print(f"Erro no download de {url}: {e}")
        except zipfile.BadZipFile:
            print(f"Erro: O arquivo {zip_filename} não é um ZIP válido ou está corrompido.")
        except Exception as e:
            print(f"Ocorreu um erro inesperado no processamento de {zip_filename}: {e}")
            
    print("Etapa de ingestão finalizada.")