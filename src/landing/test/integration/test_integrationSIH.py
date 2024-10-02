from unittest.mock import patch, MagicMock
import taskSIH

def test_data_download_and_transfer_with_dynamic_args():
    """
    Integration test to verify if data download and transfer with azcopyDir
    work correctly, passing arguments dynamically via args.
    """

    with patch('pysus.online_data.SIH.download') as mock_download, \
         patch('os.listdir') as mock_listdir, \
         patch('taskSIH.azcopyDir') as mock_azcopyDir, \
         patch('taskSIH.monitor_cpu_usage') as mock_monitor_cpu_usage, \
         patch('argparse.ArgumentParser.parse_args', return_value=MagicMock(sink_dir="./test_sink", aztoken="test_token")):
    
        mock_download.return_value = None
        mock_listdir.return_value = ['file_test.txt']
        mock_azcopyDir.return_value = None

        uf = "CE"
        year = 2021
        month = 1
        prefix = "SIH_test"
        args = MagicMock(sink_dir="./test_sink", aztoken="test_token")

        result = taskSIH.simple_download_sih(prefix, year, month, uf, args=args)

        # Verifying if the SIH.download function was called correctly
        mock_download.assert_called_once_with(
            [uf], [year], [month], groups='RD', data_dir=f'./{prefix}/RD/{year}/{month}/{uf}'
        )
        
        # Verifying if the azcopyDir function was called correctly to move the files
        mock_azcopyDir.assert_called_once_with(
            source=f'./{prefix}/RD/2021/1/{uf}', destination=f"./test_sink/JOB_RD/2021/1/CE?test_token"
        )
        
        # Verifying if CPU monitoring was called
        mock_monitor_cpu_usage.assert_called_once()

        # Verifying if the result returned by the function was "SUCCESS"
        assert result == "SUCCESS"
