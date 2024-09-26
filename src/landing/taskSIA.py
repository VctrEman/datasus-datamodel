import argparse
import ast
from itertools import product
from utils import change_cache_directory, azcopyDir, download_data_parallel

def taskDownloadFile(prefix : str, years : list, months : list, ufs : list) -> None:
    """
    args_to_download: a list of years to download
    prefix: the prefix to save the file in the storage account
    """
    import os
    from pysus.online_data import SIA

    def simple_download_sia(year : int, month : int, uf : str = 'CE') -> str:

        data_group = 'PA'
        source ='SIA'
        result = 'ERROR'
        print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
        try:
            
            # Create directory for saving data
            #save_dir = os.path.join(os.getcwd(), 'downloaded_data', f'{uf}_{year}_{month}')
            #os.makedirs(save_dir, exist_ok=True)
            
            SIA.download([uf], [year], [month], groups=data_group, data_dir=default_download_dir)
            result =  "SUCCESS"
            return result

        except Exception as e:
            print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")
            return result
        
    default_download_dir = "./download"
    os.makedirs(default_download_dir, exist_ok=True)
    download_data_parallel(ufs, years, months, downloadFun = simple_download_sia)

    azcopyDir(source=default_download_dir, destination=args.sink_dir)
    print("finished azcopy job")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prefix", help="the prefix to write/read the file in the storage account",
                         type=str, default="/SIA")
    parser.add_argument("-s", "--sink_dir", help="the file system directory to store the data in the storage account",
                         type=str,)
    parser.add_argument("-y", "--years", help="a list of years to download",
                         type=str, default="[2019]")
    parser.add_argument("-m", "--months", help="a list of months to download",
                         type=str, default="[1]")
    args = parser.parse_args()

    ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    try:
        cache_dir = "./pysus_caching"
        change_cache_directory(cache_dir)

        #assert that the argument is a list like objct
        args.years = ast.literal_eval(args.years)
        args.months = ast.literal_eval(args.months)
        print(len(tuple(product(ufs, args.years, args.months))))
        print(args.years, args.months)

        taskDownloadFile(args.prefix, args.years, args.months, ufs)
        print("success")


    except Exception as e:
        print(e)

