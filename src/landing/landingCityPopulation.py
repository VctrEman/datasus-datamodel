from minio import Minio
from pysus.online_data import IBGE
from azure.storage.filedatalake import DataLakeServiceClient
from utils import upload_file_to_adls


# Fetch available years for population data
def get_available_years_pop(source):
    from pysus.ftp.utils import zfill_year

    return sorted(set([zfill_year(f.name[-2:]) for f in IBGE.ibge.get_files(source=source)]))

def download_file() -> None:

    from tempfile import TemporaryDirectory
    import pandas as pd
    from utils import get_sink_available_data, get_download_args

    source='IBGE'
    data_group = 'POP'

    #args kind = list of years
    #if path does not exist, download, assumes that theres no data
    sink_args = get_sink_available_data('/IBGEPOP', file_ending='.csv') #assumes that everytgubg before .csv is part on the argument to get data
    source_args = get_available_years_pop('POP') 
    files_to_download = get_download_args( source_available_files = source_args, sink_available_files = sink_args) #returns a flag and a list of args

    if files_to_download[0]:
        with TemporaryDirectory() as tmp_dir:
            df = []
            for year in files_to_download[1]:
                df.append(IBGE.get_population(source='POP', year=year))

            pd.concat(df).to_csv(f'{tmp_dir}/{year}.csv', index=False)

            #returns the file name of the latest argument (year)
            upload_file_to_adls(f'{tmp_dir}/{year}.csv', f'/{source+data_group}/{year}.csv')

    else:
        print("No data to download")

def main():
    download_file()

if __name__ == "__main__":
    main()

