import os
from dotenv import load_dotenv
from utils import download_data_parallel
from minio import Minio

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

def download_sih_data(year : int, month : int, uf : str = 'CE') -> str:

    import glob
    from tempfile import TemporaryDirectory
    from pysus.online_data import SIH
    from utils import upload_to_minio

    data_group = 'RD'
    source ='SIH'
    result = 'ERROR'

    try:
        print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
        
        with TemporaryDirectory() as temp_dir:
            SIH.download([uf], [year], [month], groups=data_group, data_dir=temp_dir)
            
            # List files in the temporary directory
            files = glob.glob(temp_dir + "/" + "*.parquet/*.parquet")
            print("Number of files: ",len(files))
            prefix = f'{source}/{data_group}{uf}{str(year)}{str(month).zfill(2)}/'

            print(prefix)

            # Upload each file to MinIO
            for file_path in files:
                name = prefix + os.path.basename(file_path)
                print(name)
                upload_to_minio(file_path, BUCKET, name, minio_client)

        result =  "SUCCESS"
                
    except Exception as e:
        print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")
    
    return result

def main():
    
    ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'][:2]
    months = [1]
    years = [2019]

    download_data_parallel(ufs, years, months, downloadFun = download_sih_data)


if __name__ == "__main__":
    main()