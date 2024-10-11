import os
import logging
import ast
from dotenv import load_dotenv
from taskDownloader import task_download_files
from utils import change_cache_directory
from pysus.online_data import SIH


load_dotenv()
prefix_sih = os.getenv("PREFIX_SIH")
data_group_sih = os.getenv("DATA_GROUP_SIH")
years = ast.literal_eval(os.getenv("YEARS"))
months = ast.literal_eval(os.getenv("MONTHS"))
ufs = ast.literal_eval(os.getenv("UFS"))
cache_dir = os.getenv("CACHE_DIR")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    change_cache_directory(cache_dir)
    logger.info("Starting SIH data download...")
    task_download_files(prefix=prefix_sih, years=years, months=months, ufs=ufs, data_group=data_group_sih, download_function=SIH.download)
    logger.info("SIH download completed!")