import os
from time import time
import logging
from dotenv import load_dotenv
from utils import azcopyDir, monitor_cpu_usage

# logger = logging.getLogger(__name__)
logger = logging.getLogger('taskDownloader')
load_dotenv("../.env")

def simple_download(prefix: str, year: int, month: int, uf: str, data_group: str, download_function) -> str:
    start_time = time()
    result = 'ERROR'
    logger.info(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}, Group: {data_group}")
    try:
        sink_base_dir = os.getenv("SINK_BASE_DIR")
        sas_token = os.getenv("SAS_TOKEN")
        prefix_download = f"./{prefix}/{data_group}/{year}/{month}/{uf}"
        DIR = f"JOB_{data_group}/{year}/{month}/{uf}"
        sink_dir = f"{sink_base_dir}/{DIR}?{sas_token}"
        
        download_function([uf], [year], [month], groups=data_group, data_dir=prefix_download)
        logger.info(f"Downloaded files: {os.listdir(prefix_download)}")
        azcopyDir(source=prefix_download, destination=sink_dir)
        logger.info(f"Finished azcopy job in {time() - start_time} seconds")
        monitor_cpu_usage()
        result = "SUCCESS"
    except Exception as e:
        logger.error(f"Failed to download data: {str(e)}")
    
    return result

def download_task(uf, year, month, prefix, data_group, download_function):
    return simple_download(prefix, year, month, uf, data_group, download_function)

def task_download_files(prefix: str, years: list, months: list, ufs: list, data_group: str, download_function) -> None:
    from utils import download_data_parallel
    # download_data_parallel(
    #     prefix=prefix,
    #     ufs=ufs,
    #     years=years,
    #     months=months,
    #     downloadFun=lambda uf, year, month: simple_download(prefix, year, month, uf, data_group, download_function)
    # )
    tasks = [(uf, year, month, prefix, data_group, download_function) for uf in ufs for year in years for month in months]
    download_data_parallel(tasks, download_task)