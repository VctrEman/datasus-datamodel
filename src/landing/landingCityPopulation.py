from minio import Minio
from pysus.online_data import IBGE
from utils import upload_file_to_adls


# Fetch available years for population data
def get_available_years_pop(source):
    from pysus.ftp.utils import zfill_year

    return sorted(set([zfill_year(f.name[-2:]) for f in IBGE.ibge.get_files(source=source)]))

def taskDownloadFile(args_to_download : list, prefix : str) -> None:
    from tempfile import TemporaryDirectory
    import pandas as pd

    with TemporaryDirectory() as tmp_dir:
        df = []
        for arg in args_to_download:
            df.append(IBGE.get_population(source='POP', year=arg))

        pd.concat(df).to_csv(f'{tmp_dir}/{arg}.csv', index=False)

        #returns the file name as the latest argument for this case (simple overwrite) (year)
        upload_file_to_adls(f'{tmp_dir}/{arg}.csv', f'/{prefix}/{arg}.csv')


def download_file() -> None:
    from utils import get_sink_available_data, get_download_args

    source='IBGE'
    data_group = 'POP'
    prefix = source + data_group

    #args kind = list of years
    #if path does not exist, download, assumes that theres no data
    sink_args = get_sink_available_data('/IBGEPOP', file_ending='.csv') #assumes that everytgubg before .csv is part on the argument to get data
    source_args = get_available_years_pop('POP') 
    files_args_to_download = get_download_args( source_available_files = source_args, sink_available_files = sink_args) #returns a flag and a list of args

    if files_args_to_download[0]:
        taskDownloadFile(args_to_download = files_args_to_download[1], prefix = prefix )

    else:
        print("No data to download")

def main():
    download_file()

if __name__ == "__main__":
    main()

