import os
from dotenv import load_dotenv
from utils import download_data_parallel
from pysus.online_data import SIA

load_dotenv()

ufs = ['ac', 'al', 'ap', 'am','ba', 'ce', 'df', 'es', 'go', 'ma', 'mt', 'ms', 'mg', 'pa', 'pb', 'pr', 'pe', 'pi', 'rj', 'rn', 'rs', 'ro', 'rr', 'sc', 'sp', 'se', 'to']
months = [1]
years = [2019]
data_group = ['PA']
dbfs_raw_path = "./adls/bronze/sia"

# Example usage
#ufs = ['ac', 'al', 'ap', 'am','ba', 'ce', 'df', 'es', 'go', 'ma', 'mt', 'ms', 'mg', 'pa', 'pb', 'pr', 'pe', 'pi', 'rj', 'rn', 'rs', 'ro', 'rr', 'sc', 'sp', 'se', 'to']
#years = [2019]
#months = [1] #list(range(1, 13))
#ata_group = ['PA']
# Path to store the raw data in DBFS
#dbfs_raw_path = "./adls/bronze"

# Function to download SIA data
def download_sia_data(year, month, uf):
    try:
        print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
        SIA.download([uf], [year], [month], groups= data_group, data_dir= dbfs_raw_path)
    except Exception as e:
        print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")

# Parallel download function call
download_data_parallel(ufs, years, months, dbfs_raw_path, downloadFun = download_sia_data)