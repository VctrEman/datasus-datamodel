import os
import s3fs
import json
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv
from pathlib import Path


load_dotenv('src/landing/.env')

def configure_s3fs():
    STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
    STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
    STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")

    if not all([STORAGE_ENDPOINT, STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY]):
        raise ValueError("MinIO environment variables are not set correctly.")

    s3 = s3fs.S3FileSystem(
        anon=False,
        key=STORAGE_ACCESS_KEY,
        secret=STORAGE_SECRET_KEY,
        client_kwargs={'endpoint_url': f'http://{STORAGE_ENDPOINT}'}
    )
    return s3

# Configuração da API do Kaggle
def configure_kaggle_api():
    api = KaggleApi()
    api.authenticate()
    return api

def list_parquet_files(s3, bucket, base_prefix):
    all_files = s3.glob(f"{bucket}/{base_prefix}/**/*.parquet.zst")
    if not all_files:
        raise FileNotFoundError(f"No .parquet.zst files found in {bucket}/{base_prefix}")
    print(f"Found {len(all_files)} files to download.")
    return all_files

def download_files(s3, files, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    for file_path in files:
        local_path = Path(local_dir) / Path(file_path).name
        s3.get(file_path, str(local_path))
        print(f"Downloaded {file_path} to {local_path}")

def upload_to_kaggle(api, dataset_path, title, private=True):
    try:
        print(f"Contents of {dataset_path} before upload:")
        for file in Path(dataset_path).glob('*'):
            print(file)
        
        # Fazer upload
        api.dataset_create_new(folder=dataset_path, public=not private, dir_mode="zip")
        print(f"Dataset uploaded to Kaggle with title '{title}'")
        
    except Exception as e:
        print(f"Failed to upload dataset to Kaggle: {e}")
        print("Verify Kaggle API credentials and configuration.")

bucket_name = 'silver'
base_prefix = 'SIA' 
local_download_dir = './kaggle_upload'
dataset_title = "silver_sia_dataset"
kaggle_metadata = {
    "id": "nycolasdias2/silver_sia_dataset",
    "title": dataset_title,
    "licenses": [{"name": "CC0-1.0"}]
}

# Configurar conexões
s3 = configure_s3fs()
api = configure_kaggle_api()


file_paths = list_parquet_files(s3, bucket_name, base_prefix)
download_files(s3, file_paths, local_download_dir)

if not file_paths:
    raise FileNotFoundError("Nenhum arquivo .parquet.zst foi baixado do MinIO. Verifique o bucket e o prefixo.")

metadata_file = Path(local_download_dir) / "dataset-metadata.json"
with open(metadata_file, "w") as f:
    json.dump(kaggle_metadata, f)
print(f"Saved metadata to {metadata_file}")

upload_to_kaggle(api, local_download_dir, dataset_title)