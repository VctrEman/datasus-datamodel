from collections.abc import Callable
from tqdm import tqdm
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from minio import Minio
from minio.error import S3Error

# Concurrent download
def download_data_parallel(ufs : list, years, months : list, downloadFun :  Callable):

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

def upload_to_minio(file_path : str, bucket_name : str, object_name : str, minio_client : Minio):
    """
    Uploads a file to MinIO.

    :param file_path: Path to the file to upload
    :param bucket_name: The name of the bucket in MinIO
    :param object_name: The name the file will have in the bucket (object name)
    """
    try:
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
def upload_file_to_adls(file_path : str, file_system_client, destination_path : str):
    """
    tales a file, a file system client, and a destination path and uploads the file to the specified destination path in ADLS2.
    """
    # Create a file client
    file_client = file_system_client.get_file_client(destination_path)

    # Upload CSV data as bytes
    file_client.upload_data(file_path, overwrite=True)
    with open(file_path, 'rb') as file:
        file_contents = file.read()
        file_client.upload_data(file_contents, overwrite=True)