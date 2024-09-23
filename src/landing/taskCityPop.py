import argparse
import ast
from utils import change_cache_directory, upload_file_to_adls, upload_file_to_minio


def taskDownloadFile(args_to_download : list, prefix : str) -> None:
    """
    args_to_download: a list of years to download
    prefix: the prefix to save the file in the storage account
    """
    from tempfile import TemporaryDirectory
    import pandas as pd
    from pysus.online_data import IBGE

    with TemporaryDirectory() as tmp_dir:
        df = []
        for arg in args_to_download:
            df.append(IBGE.get_population(source='POP', year=arg))

        pd.concat(df).to_csv(f'{tmp_dir}/{arg}.csv', index=False)

        #returns the file name as the latest argument for this case (simple overwrite) (year)
        upload_file_to_minio(f'{tmp_dir}/{arg}.csv', f'/{prefix}/{arg}.csv')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--args_to_download", help="a list of years to download", type=str, #must be a string for proper conversion -> yes
                        default="[2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]")
    parser.add_argument("-p", "--prefix", help="the prefix to write/read the file in the storage account", type=str,
                        default="/IBGEPOP")
    args = parser.parse_args()


    try:
        # Change the cache directory to the specified path
        change_cache_directory("./pysus_caching")

        #assert that the argument is a list like objct
        args.args_to_download = ast.literal_eval(args.args_to_download)
        args.args_to_download = list(args.args_to_download)
        print(args.args_to_download)

        taskDownloadFile(args.args_to_download, args.prefix)
        print("success")

    except Exception as e:
        print(e)
