import argparse
import ast
from itertools import product
from utils import change_cache_directory, upload_file_to_adls, upload_file_to_minio, download_data_parallel

def taskDownloadFile(prefix : str, years : list, months : list, ufs : list) -> None:
    """
    args_to_download: a list of years to download
    prefix: the prefix to save the file in the storage account
    """
    import os
    from tempfile import TemporaryDirectory
    from pysus.online_data import SIH
    import glob

    def simple_download_sih(year : int, month : int, uf : str = 'CE') -> str:

        data_group = 'RD'
        source ='SIH'
        result = 'ERROR'
        print(f"Downloading data for UF: {uf}, Year: {year}, Month: {month}")
        try:
            
            with TemporaryDirectory() as temp_dir:
                SIH.download([uf], [year], [month], groups=data_group, data_dir=temp_dir)
                
                # List files in the temporary directory
                files = glob.glob(temp_dir + "/" + "*.parquet/*.parquet")
                print("Number of files: ",len(files))
                prefix = f'{source}/{data_group}{uf}{str(year)}{str(month).zfill(2)}/'
                print(prefix)

                # Upload each file to MinIO
                for file_path in files:
                    name = prefix + os.path.basename(file_path)
                    print(name)
                    upload_file_to_adls(file_path, name)
            result =  "SUCCESS"
            return result

        except Exception as e:
            print(f"Failed to download data for UF: {uf}, Year: {year}, Month: {month}: {str(e)}")
            return result
    
    download_data_parallel(ufs, years, months, downloadFun = simple_download_sih)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prefix", help="the prefix to write/read the file in the storage account", type=str,
                        default="/SIH")
    parser.add_argument("-y", "--years", help="a list of years to download", type=str,
                        default="[2019]")
    parser.add_argument("-m", "--months", help="a list of months to download", type=str,
                        default="[1]")
    args = parser.parse_args()

    ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    try:
        # Change the cache directory to the specified path
        change_cache_directory("./pysus_caching")

        #assert that the argument is a list like objct
        args.years = ast.literal_eval(args.years)
        args.months = ast.literal_eval(args.months)
        print(len(tuple(product(ufs, args.years, args.months))))

        taskDownloadFile(args.prefix, args.years, args.months, ufs)
        print("success")

    except Exception as e:
        print(e)

