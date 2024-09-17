import os
import logging
import gzip
import json
import pandas as pd
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

dbfs_raw_path = "./adls/bronze/"
URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_ibge_data(state_filter=None):
    """
    Extrai dados do IBGE e retorna um DataFrame com cidades e estados, aplicando filtro opcional.
    """
    try:
        request = Request(URL)
        request.add_header('Accept-encoding', 'gzip')
        with urlopen(request) as response:
            data = json.loads(gzip.decompress(response.read()).decode('utf-8')) if response.info().get('Content-Encoding') == 'gzip' else json.loads(response.read().decode('utf-8'))
    except (HTTPError, URLError) as e:
        logging.error(f"Error fetching data: {e}")
        return pd.DataFrame()

    cities = []
    states = {}
    
    for obj in data:
        state_abbr = obj["microrregiao"]["mesorregiao"]["UF"]["sigla"]
        
        if state_filter and state_abbr != state_filter:
            continue
        
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
    
    if "state_abbr" not in cities_df.columns or "state_abbr" not in states_df.columns:
        logging.error("A coluna 'state_abbr' está ausente em um dos DataFrames.")
        return pd.DataFrame()

    merged_df = pd.merge(cities_df, states_df, on="state_abbr")
    return merged_df

def main():
    merged_data = fetch_ibge_data()
    if not merged_data.empty:
        if not os.path.exists(dbfs_raw_path):
            os.makedirs(dbfs_raw_path)
        merged_data.to_csv(dbfs_raw_path + "localization.csv", index=False)
        logging.info("Arquivo CSV salvo com sucesso.")
    else:
        logging.error("Nenhum dado foi retornado para salvar.")

if __name__ == "__main__":
    main()