import os
from taskDownloader import task_download_files
from pysus.online_data import SIH

def test_download_data_parallel():
    ufs = ["CE"]  
    years = [2008]  
    months = [1] 
    prefix = './SIH'  
    data_group = 'RD'  
    download_function = SIH.download  

    download_dir = f"./{prefix}/{data_group}/{years[0]}/{months[0]}/{ufs[0]}"

    if os.path.exists(download_dir):
        for f in os.listdir(download_dir):
            os.remove(os.path.join(download_dir, f))
    else:
        os.makedirs(download_dir)

    task_download_files(prefix, years, months, ufs, data_group, download_function)

    downloaded_files = os.listdir(download_dir)
    assert len(downloaded_files) > 0, "Nenhum arquivo foi baixado."

    for file_name in downloaded_files:
        file_path = os.path.join(download_dir, file_name)
        
        assert os.path.exists(file_path), f"O arquivo {file_name} não foi encontrado."

        assert os.path.getsize(file_path) > 0, f"O arquivo {file_name} está vazio."