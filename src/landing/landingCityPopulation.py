from dotenv import load_dotenv
from minio import Minio
import os
from pysus.online_data import IBGE

load_dotenv()

STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
BUCKET = os.getenv("STORAGE_BUCKET")

minio_client = Minio(
    endpoint=STORAGE_ENDPOINT,
    access_key=STORAGE_ACCESS_KEY,
    secret_key=STORAGE_SECRET_KEY,
    secure=False  # Non HTTPS
)

# Fetch available years
def get_available_years(source):
    from pysus.ftp.utils import zfill_year

    return sorted(set([zfill_year(f.name[-2:]) for f in IBGE.ibge.get_files(source=source)]))

def download_file():

    from tempfile import TemporaryDirectory
    import pandas as pd
    from utils import upload_to_minio

    source='IBGE'
    data_group = 'POP'

    with TemporaryDirectory() as temp_dir:

        pop_list = [IBGE.get_population(year, source="POP") for year in get_available_years("POP")]

        df = pd.concat(pop_list)
        prefix = f'{source}/{data_group}/'

        print(prefix)

        name = prefix + 'population.csv'
        file_path = temp_dir + '/' + 'population.csv'
        df.to_csv(file_path,index=False)
        upload_to_minio(file_path, BUCKET, name, minio_client)

def main():
    download_file()

if __name__ == "__main__":
    main()

