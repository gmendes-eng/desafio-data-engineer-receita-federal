# src/ingestion.py

import requests
import zipfile
import logging
from pathlib import Path
from pyspark.sql import SparkSession

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL base com a pasta de data
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"

URLS = [f"{BASE_URL}Empresas0.zip", f"{BASE_URL}Socios0.zip"]

ARCHIVES_PATH = Path("data/bronze/landing/archives")
UNZIPPED_PATH = Path("data/bronze/landing/unzipped_csvs")
RAW_PATH = Path("data/bronze/raw")

def baixar_arquivos_zipados():
    """
    BRONZE | ETAPA 1:
    Baixa os arquivos .zip da fonte e os salva na pasta 'archives' da Landing Zone.
    """
    logging.info("--- 1.1: Iniciando download dos arquivos .zip ---")
    ARCHIVES_PATH.mkdir(parents=True, exist_ok=True)
    
    for url in URLS:
        zip_filename = url.split("/")[-1]
        zip_filepath = ARCHIVES_PATH / zip_filename
        
        try:
            logging.info(f"Baixando: {zip_filename}...")
            # O User-Agent ajuda a simular um navegador, evitando bloqueios simples.
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
            
            with open(zip_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Download de {zip_filename} concluído com sucesso.")

        except Exception as e:
            logging.error(f"FALHA CRÍTICA no download do arquivo {zip_filename}: {e}")
            raise e
    logging.info("--- 1.1: Download de todos os arquivos finalizado ---")

def descompactar_arquivos():
    """
    BRONZE | ETAPA 2:
    Descompacta os arquivos da pasta 'archives' para a pasta 'unzipped_csvs'.
    """
    logging.info("--- 1.2: Iniciando descompactação dos arquivos ---")
    UNZIPPED_PATH.mkdir(parents=True, exist_ok=True)

    try:
        # Itera sobre todos os arquivos .zip que foram baixados na pasta 'archives'
        for zip_filepath in ARCHIVES_PATH.glob("*.zip"):
            logging.info(f"Descompactando {zip_filepath.name}...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(UNZIPPED_PATH)
            logging.info(f"Arquivo {zip_filepath.name} descompactado.")
    
    except Exception as e:
        logging.error(f"FALHA CRÍTICA na descompactação dos arquivos: {e}")
        raise e
    logging.info("--- 1.2: Descompactação de todos os arquivos finalizada ---")

def converter_csvs_para_delta(spark: SparkSession):
    """
    BRONZE | ETAPA 3:
    Lê os CSVs da Landing Zone e os converte para o formato Delta na camada Raw.
    """
    logging.info("--- 1.3: Iniciando conversão de CSVs para Delta (Camada Raw) ---")
    RAW_PATH.mkdir(parents=True, exist_ok=True)

    try:
        # Usa wildcards (*) para encontrar os arquivos de forma flexível
        logging.info("Convertendo Empresas CSV para Delta...")
        df_empresas = spark.read.csv(str(UNZIPPED_PATH / "*EMPRECSV"), sep=';', inferSchema=False)
        df_empresas.write.format("delta").mode("overwrite").save(str(RAW_PATH / "empresas"))
        
        logging.info("Convertendo Sócios CSV para Delta...")
        df_socios = spark.read.csv(str(UNZIPPED_PATH / "*SOCIOCSV"), sep=';', inferSchema=False)
        df_socios.write.format("delta").mode("overwrite").save(str(RAW_PATH / "socios"))
        
        logging.info("CSVs convertidos para Delta na camada Raw.")
    except Exception as e:
        logging.error(f"FALHA CRÍTICA ao converter CSV para Delta: {e}")
        raise e
    logging.info("--- 1.3: Conversão para Delta finalizada com sucesso ---")