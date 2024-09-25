import os
import logging
import gzip
import json
import pandas as pd
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from utils import upload_file_to_minio
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_ibge_data(url: str) -> pd.DataFrame:
    """
    Fetch data from the IBGE API.
    :param url: URL to fetch the data from.
    :return: A DataFrame with the merged city and state data.
    """
    try:
        request = Request(url)
        request.add_header('Accept-encoding', 'gzip')
        with urlopen(request) as response:
            data = json.loads(gzip.decompress(response.read()).decode('utf-8')) if response.info().get('Content-Encoding') == 'gzip' else json.loads(response.read().decode('utf-8'))
        logging.info("Dados obtidos com sucesso da API do IBGE.")
    except (HTTPError, URLError) as e:
        logging.error(f"Erro ao buscar dados da API: {e}")
        return pd.DataFrame()

    # Process the data
    cities = []
    states = {}

    for obj in data:
        state_abbr = obj["microrregiao"]["mesorregiao"]["UF"]["sigla"]

        city = {
            "city_code": str(obj["id"])[:6],
            "city_name": obj["nome"],
            "state_abbr": state_abbr
        }
        cities.append(city)

        if state_abbr not in states:
            states[state_abbr] = {
                "state_abbr": state_abbr,
                "state_code": obj["microrregiao"]["mesorregiao"]["UF"]["id"],
                "state_name": obj["microrregiao"]["mesorregiao"]["UF"]["nome"]
            }

    cities_df = pd.DataFrame(cities)
    states_df = pd.DataFrame.from_dict(states, orient="index").reset_index(drop=True)

    return pd.merge(cities_df, states_df, on="state_abbr")

def main(dbfs_raw_path: str, url: str, output_file: str, minio_object_name: str) -> None:
    """
    Main function to fetch data, save locally, and upload to MinIO.
    :param dbfs_raw_path: Path to save the raw data.
    :param url: URL to fetch the data from.
    :param output_file: Name of the output CSV file.
    :param minio_object_name: Object name for the file in MinIO.
    """
    merged_data = fetch_ibge_data(url)

    if not merged_data.empty:
        if not os.path.exists(dbfs_raw_path):
            os.makedirs(dbfs_raw_path)
            logging.info(f"Diretório criado: {dbfs_raw_path}")
        
        parquet_path = os.path.join(dbfs_raw_path, output_file)
        merged_data.to_parquet(parquet_path, index=False)
        logging.info(f"Arquivo Parquet salvo com sucesso: {parquet_path}")
        
        upload_file_to_minio(parquet_path, minio_object_name)
        logging.info(f"Upload do arquivo parquet para o MinIO feito com sucesso: {minio_object_name}")
    else:
        logging.error("Nenhum dado retornado da API para salvar.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch IBGE data and upload to MinIO.")
    
    parser.add_argument("-d", "--dbfs_raw_path", type=str, default="./adls/bronze/", help="Caminho para salvar o arquivo CSV.")
    parser.add_argument("-u", "--url", type=str, default="https://servicodados.ibge.gov.br/api/v1/localidades/municipios", help="URL da API do IBGE.")
    parser.add_argument("-o", "--output_file", type=str, default="localization.parquet", help="Nome do arquivo CSV de saída.")
    parser.add_argument("-m", "--minio_object_name", type=str, default="localization.parquet", help="Nome do objeto no MinIO.")
    
    args = parser.parse_args()
    
    main(args.dbfs_raw_path, args.url, args.output_file, args.minio_object_name)