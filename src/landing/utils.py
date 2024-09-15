
from tqdm import tqdm
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Parallel download function with progress tracking
def download_data_parallel(ufs, years, months, dbfs_raw_path, downloadFun):

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