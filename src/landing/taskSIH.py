import argparse
import ast
from itertools import product
from utils import change_cache_directory, azcopyDir, download_data_parallel, monitor_cpu_usage

def simple_download_sih(prefix: str, year : int, month : int, uf : str = 'CE', args=None) -> str:
    import os
    from pysus.online_data import SIH
    from time import time
    start_time = time()

    data_group = 'RD'
    result = 'ERROR'
    print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
    try:
        prefix_download = f"./{prefix}/{data_group}/{year}/{month}/{uf}"
        
        DIR = f"JOB_{data_group}/{year}/{month}/{uf}"
        sink_dir = f"{args.sink_dir}/{DIR}?{args.aztoken}" 
               
        SIH.download([uf], [year], [month], groups=data_group, data_dir=prefix_download)
        print(os.listdir(prefix_download))
        print("azcopydir: ", prefix_download)
        print("sink dir: ", f"{args.sink_dir}/{DIR}")
        azcopyDir(source=prefix_download, destination=sink_dir)
        print("finished azcopy job")
        print("texec: ", time() - start_time)
        monitor_cpu_usage()
        result = "SUCCESS"
        return result

    except Exception as e:
        print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")
        return result


def taskDownloadFile(prefix : str, years : list, months : list, ufs : list) -> None:
    """
    args_to_download: a list of years to download
    prefix: the prefix to save the file in the storage account
    """
    download_data_parallel(prefix=prefix, ufs=ufs, years=years, months=months, downloadFun = simple_download_sih)


if __name__ == '__main__':
    ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prefix", help="the prefix to write/read the file in the storage account", type=str,
                        default="./SIH")
    parser.add_argument("-s", "--sink_dir", help="the file system directory to store the data in the storage account",
                        type=str,)
    parser.add_argument("-t", "--aztoken", help="the file system directory to store the data in the storage account",
                         type=str, default="my_sas_token")
    parser.add_argument("-y", "--years", help="a list of years to download", type=str,
                        default="[2019]")
    parser.add_argument("-m", "--months", help="a list of months to download", type=str,
                        default="[1]")
    parser.add_argument("-u", "--ufs", help="a list of ufs to download", type=str,
                    default=str(ufs))
    args = parser.parse_args()

    try:
        cache_dir = "./pysus_caching"
        change_cache_directory(cache_dir)

        #assert that the argument is a list like objct
        args.ufs = ast.literal_eval(args.ufs)
        args.years = ast.literal_eval(args.years)
        args.months = ast.literal_eval(args.months)
        print(len(tuple(product(args.ufs, args.years, args.months))))
        print(args.years, args.months, args.ufs)
        taskDownloadFile(args.prefix, args.years, args.months, args.ufs)
        print("success")

    except Exception as e:
        print(e)