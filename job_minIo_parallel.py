import os
from pathlib import Path
import shutil
from pysus.online_data import SIA
from src.landing.utils import upload_file_to_minio
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

load_dotenv('src/landing/.env')

def change_cache_directory(new_cache_path: str = "/src/caching") -> None:
    global __cachepath__
    os.makedirs(new_cache_path, exist_ok=True)
    __cachepath__ = Path(new_cache_path)
    print(f"Current cache directory: {__cachepath__}")

def download_sia_data(uf, year, month, data_dir, groups, prefix):
    """
    Função para baixar os dados do SIA e armazenar no diretório de cache especificado.
    """
    try:
        print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
        os.makedirs(data_dir, exist_ok=True)
        
        SIA.download([uf], [year], [month], groups=groups, data_dir=str(data_dir))

        downloaded_file = next(Path(data_dir).glob(f'*{uf.upper()}*.parquet'), None)
        if downloaded_file:
            destination_path = Path(f"./{prefix}/{groups[0]}/{year}/{month}/{uf}")
            destination_path.mkdir(parents=True, exist_ok=True)

            final_file_path = destination_path / downloaded_file.name
            shutil.move(str(downloaded_file), str(final_file_path))
            print(f"Data downloaded and moved to {final_file_path}")
            return final_file_path
        else:
            print(f"No .parquet file found in {data_dir} for UF: {uf}.")
            return None

    except Exception as e:
        print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")
        return None


def upload_to_minio(file_path, bucket_name):
    """
    Faz o upload do arquivo para o MinIO utilizando a função de utilidades.
    """
    try:
        path = Path(file_path)
        if path.is_dir():
            file_path = next(path.glob('*.parquet'), None)
            if not file_path:
                raise FileNotFoundError(f"No .parquet file found in directory {path}")

        # Verificar se as variáveis de ambiente estão configuradas
        STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
        STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
        STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
        BUCKET = os.getenv("STORAGE_BUCKET")

        if not all([STORAGE_ENDPOINT, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY, BUCKET]):
            raise ValueError("As variáveis de ambiente necessárias para o MinIO não estão configuradas corretamente.")

        object_name = str(Path(file_path).relative_to(Path('.')))
        upload_file_to_minio(str(file_path), object_name)
        print(f"File {file_path} uploaded to MinIO bucket {bucket_name} as {object_name}.")
    except Exception as e:
        print(f"Failed to upload {file_path} to MinIO: {str(e)}")

def download_and_upload_task(uf, year, month, data_group, prefix, raw_data_bucket):
    cache_path = download_sia_data(uf, year, month, str(__cachepath__), data_group, prefix)
    if cache_path:
        upload_to_minio(cache_path, raw_data_bucket)
    return cache_path

# Configurações
change_cache_directory("/tmp/sia_cache")

ufs = ['ac'] #['ac', 'al', 'ap', 'am', 'ba', 'ce', 'df', 'es', 'go', 'ma', 'mt', 'ms', 'mg', 'pa', 'pb', 'pr', 'pe', 'pi', 'rj', 'rn', 'rs', 'ro', 'rr', 'sc', 'sp', 'se', 'to']
years = [2009]
months = [1] #[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
data_group = ['PA']
prefix = 'SIA'
raw_data_bucket = 'test'

start_time = time.time()
with ProcessPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(download_and_upload_task, uf, year, month, data_group, prefix, raw_data_bucket)
        for uf in ufs for year in years for month in months
    ]

    for future in as_completed(futures):
        result = future.result()
        if result:
            print(f"Download and upload completed for: {result}")
        else:
            print("Download or upload failed.")

end_time = time.time()
print(f"Total execution time: {end_time - start_time:.2f} seconds")
