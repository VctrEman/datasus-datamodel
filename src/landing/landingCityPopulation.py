from dotenv import load_dotenv
from minio import Minio
import os
from pysus.online_data import IBGE
from azure.storage.filedatalake import DataLakeServiceClient
from utils import upload_file_to_adls

load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("AZSTORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZSTORAGE_ACCOUNT_KEY")
FILE_SYSTEM_NAME = os.getenv("AZFILE_SYSTEM_NAME")


service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)

file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM_NAME)

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
        upload_file_to_adls(file_path, file_system_client, name)

def main():
    download_file()

if __name__ == "__main__":
    main()

