from taskSIA import simple_download_sia, taskDownloadFile
from unittest.mock import patch, MagicMock

def test_simple_download_sia():
    with patch('pysus.online_data.SIA.download', return_value=None), \
         patch('taskSIA.azcopyDir'), \
         patch('taskSIA.monitor_cpu_usage'), \
         patch('os.listdir', return_value=['file1.csv', 'file2.csv']), \
         patch('os.path.exists', return_value=True):
        
        mock_args = MagicMock()
        mock_args.sink_dir = "/mock/sink/dir"
        mock_args.aztoken = "mock_token"
        prefix = "SIA"
        year = [2008]
        month = [1]
        uf = "CE"
        
        result = simple_download_sia(prefix=prefix, year=year, month=month, uf=uf, args=mock_args)
        assert result == "SUCCESS"
        

def test_taskDownloadFile():
    with patch('taskSIA.download_data_parallel') as mock_download:
        prefix = "SIA"
        years = [2008]
        months = [1]
        ufs = ["CE"]

        taskDownloadFile(prefix=prefix, years=years, months=months, ufs=ufs)
        mock_download.assert_called_once_with(prefix=prefix, ufs=ufs, years=years, months=months, downloadFun=simple_download_sia)