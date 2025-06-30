import os
from time import time
import logging
from dotenv import load_dotenv
from utils import monitor_cpu_usage

logger = logging.getLogger('taskDownloader')
load_dotenv("../.env")

def simple_download(prefix: str, year: int, month: int, uf: str, data_group: str, download_function) -> str:
    start_time = time()
    result = 'ERROR'
    logger.info(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}, Group: {data_group}")
    try:
        prefix_download = f"./{prefix}/{data_group}/{year}{month:02}"
        
        download_function([uf], [year], [month], groups=data_group, data_dir=prefix_download)
        logger.info(f"Downloaded files: {os.listdir(prefix_download)}")
        monitor_cpu_usage()
        result = "SUCCESS"
    except Exception as e:
        logger.error(f"Failed to download data: {str(e)}")
    
    return result

def download_task(uf, year, month, prefix, data_group, download_function):
    return simple_download(prefix, year, month, uf, data_group, download_function)

def task_download_files(prefix: str, years: list, months: list, ufs: list, data_group: str, download_function) -> None:
    from utils import download_data_parallel
    tasks = [(uf, year, month, prefix, data_group, download_function) for uf in ufs for year in years for month in months]
    download_data_parallel(tasks, download_task)