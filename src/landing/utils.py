from collections.abc import Callable
from tqdm import tqdm
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from pysus.ftp import __cachepath__
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv("../.env")

# Concurrent download
def download_data_parallel(ufs : list, years : list, months : list, downloadFun :  Callable) -> None:

        # Record the start time of the job
    start_time = time.time()
    
    # Calculate total tasks
    total_tasks = len(ufs) * len(years) * len(months)

    # Initialize error counter
    error_count = 0

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=100) as executor, tqdm(total=total_tasks) as progress_bar:
        # Using a list to store download tasks
        futures = [
            executor.submit(downloadFun, year, month, uf)
            for uf in ufs for year in years for month in months
        ]

        # Process the tasks as they are completed
        for future in as_completed(futures):
            result = future.result()

            # Check if the task was successful
            if result is None:  # Assume None represents failure
                error_count += 1

            # Update the progress bar for each completed task
            progress_bar.update(1)

    # Print summary of download errors
    print(f"All downloads completed, errors: {error_count}")

    # Record the end time of the job
    end_time = time.time()
    # Calculate the total execution time
    total_time = end_time - start_time
    print(f"Total execution time: {total_time:.2f} seconds")

def upload_file_to_minio(file_path : str, object_name : str) -> None:
    """
    Uploads a file to MinIO.

    :param file_path: Path to the file to upload
    :param bucket_name: The name of the bucket in MinIO
    :param object_name: The name the file will have in the bucket (object name)
    """
    try:
        STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT")
        STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
        STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
        BUCKET = os.getenv("STORAGE_BUCKET")
        bucket_name = BUCKET

        minio_client = Minio(
            endpoint=STORAGE_ENDPOINT,
            access_key=STORAGE_ACCESS_KEY,
            secret_key=STORAGE_SECRET_KEY,
            secure=False  # Non HTTPS
        )
        # Check if the bucket exists; create it if it does not exist
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        
        # Upload the file
        with open(file_path, 'rb') as file_data:
            file_stat = os.stat(file_path)
            minio_client.put_object(
                bucket_name,
                object_name,
                file_data,
                file_stat.st_size,
                content_type='application/octet-stream'
            )
        print(f"Successfully uploaded '{object_name}' to bucket '{bucket_name}'.")
    
    except S3Error as err:
        print(f"Error uploading file to MinIO: {err}")

# Upload data to ADLS2
def upload_file_to_adls(file_path : str, destination_path : str) -> None:
    """
    takes a file, a file system client, and a destination path and uploads the file to the specified destination path in ADLS2.
    """
    from azure.storage.filedatalake import DataLakeServiceClient
    import os

    STORAGE_ACCOUNT_NAME = os.getenv("AZSTORAGE_ACCOUNT_NAME") 
    STORAGE_ACCOUNT_KEY = os.getenv("AZSTORAGE_ACCOUNT_KEY") 
    FILE_SYSTEM_NAME = os.getenv("AZFILE_SYSTEM_NAME") 


    service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=STORAGE_ACCOUNT_KEY
    )

    file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM_NAME)

    # Create a file client
    file_client = file_system_client.get_file_client(destination_path)

    # Upload CSV data as bytes
    file_client.upload_data(file_path, overwrite=True)
    with open(file_path, 'rb') as file:
        file_contents = file.read()
        file_client.upload_data(file_contents, overwrite=True)


def get_sink_available_data(directory_name : str, file_ending : str = '.csv') -> list:

    from azure.storage.filedatalake import DataLakeServiceClient
    import os

    def initialize_storage_account(storage_account_name, storage_account_key) -> None:
        try:  
            global service_client
            service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=storage_account_key
        )
        except Exception as e:
            print(e)

    def list_directory_contents(file_system_name, directory_name) -> list:
        paths = []
        try:
            file_system_client = service_client.get_file_system_client(file_system_name)
            paths = file_system_client.get_paths(path=directory_name)
            return [(path.name, path.name.split(file_ending)[0] ) for path in paths if file_ending in path.name ]

        except Exception as e:
            print(e)

        return paths

    STORAGE_ACCOUNT_NAME = os.getenv("AZSTORAGE_ACCOUNT_NAME") 
    STORAGE_ACCOUNT_KEY = os.getenv("AZSTORAGE_ACCOUNT_KEY") 
    FILE_SYSTEM_NAME = os.getenv("AZFILE_SYSTEM_NAME") 

    initialize_storage_account(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)

    # List the contents of the directory
    file_system_name = FILE_SYSTEM_NAME
    available_files = list(list_directory_contents(file_system_name, directory_name))

    return available_files



def get_download_args(source_available_files : list, sink_available_files: list) -> tuple[bool, list]:
    """
    args: available_files: list of tuples with the available files in the storage account

    returns: (flag to indicate if there is new data to process, list of years to download)
    """
    has_data_to_process = False
    download_args = []

    sink_args = [arg[1] for arg in sink_available_files] 

    try :
        download_args = source_available_files
        new_args = set(download_args) - set(sink_args)

    except Exception as e:
        print("No data to consider downloading, exception: ", e)
        max_arg = 0
        return has_data_to_process

    if len(sink_args) == 0: has_data_to_process = True

    #case when there is data to process, return the flag and the new_args
    elif len(new_args) > 0: has_data_to_process = True

    return has_data_to_process, new_args


def get_available_years_pop(source):
    from pysus.online_data import IBGE
    from pysus.ftp.utils import zfill_year

    return sorted(set([zfill_year(f.name[-2:]) for f in IBGE.ibge.get_files(source=source)]))


def change_cache_directory(new_cache_path: str = "/src/caching") -> None:
    
    """
    Changes the caching directory to the specified path.
    """
    global __cachepath__
    os.makedirs(new_cache_path, exist_ok=True)
    __cachepath__ = Path(new_cache_path)
    print(f"Current cache directory: {__cachepath__}")

def azcopyDir(source, destination):
    os.system(f"azcopy copy '{source}' '{destination}' --recursive")