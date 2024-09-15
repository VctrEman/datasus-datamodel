from tqdm import tqdm
import os
from pysus.online_data import SIA
from concurrent.futures import ThreadPoolExecutor, as_completed
import time



# Function to download SIA data for a specific year, month, and UF
def download_sia_data(year, month, uf):
    try:
        print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
        SIA.download([uf], [year], [month], groups= data_group, data_dir= dbfs_raw_path)
    except Exception as e:
        print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")
        
# Parallel download function with progress tracking
def download_sia_data_parallel(ufs, years, months):

        # Record the start time of the job
    start_time = time.time()


    # Ensure the destination folder exists
    if not os.path.exists(dbfs_raw_path):
        os.makedirs(dbfs_raw_path)
    
    # Calculate total tasks
    total_tasks = len(ufs) * len(years) * len(months)

    # Initialize error counter
    error_count = 0

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=100) as executor, tqdm(total=total_tasks) as progress_bar:
        # Using a list to store download tasks
        futures = [
            executor.submit(download_sia_data, year, month, uf)
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

# Example usage
ufs = ['ac', 'al', 'ap', 'am','ba', 'ce', 'df', 'es', 'go', 'ma', 'mt', 'ms', 'mg', 'pa', 'pb', 'pr', 'pe', 'pi', 'rj', 'rn', 'rs', 'ro', 'rr', 'sc', 'sp', 'se', 'to']
years = [2019]
months = [1] #list(range(1, 13))
data_group = ['PA']
# Path to store the raw data in DBFS
dbfs_raw_path = "./adls/bronze"

# Call the parallel download function
download_sia_data_parallel(ufs, years, months)