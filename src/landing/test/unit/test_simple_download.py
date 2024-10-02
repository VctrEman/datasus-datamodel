from taskSIA import simple_download_sia, taskDownloadFile
from unittest.mock import patch

def test_simple_download_sia_falure():
    with patch('pysus.online_data.SIA.download') as mock_download:
            
        mock_download.side = Exception("Download Failed")
        
        prefix="SIA"
        year=[2008]
        month=[1]
        uf=["CE"]
        
        result = simple_download_sia(prefix, year, month, uf)
        assert result == "ERROR"
        
        
def test_task_download_file():
    with patch('taskSIA.download_data_parallel') as mock_download_data_parallel:
        mock_download_data_parallel.side_effect = Exception("Download Failed")
        
        prefix="SIA"
        year=[2008]
        month=[2]
        uf=["PI"]
        
        try: 
            taskDownloadFile(prefix, year, month, uf)            
        except Exception as e:
            assert str(e) == "Download Failed"