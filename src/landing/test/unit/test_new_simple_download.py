import os
from unittest.mock import patch
from taskDownloader import simple_download

def fake_download_function(ufs, years, months, groups, data_dir):
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, 'test_file.csv'), 'w') as f:
        f.write('mock data')

def test_simple_download_output(tmpdir):
    prefix = "mock/prefix"
    year = 2021
    month = 1
    uf = "CE"
    data_group = "PA"
    
    sink_base_dir = tmpdir.mkdir("sink_base")
    expected_sink_dir = sink_base_dir / f"JOB_{data_group}/{year}/{month}/{uf}"
    os.environ["SINK_BASE_DIR"] = str(sink_base_dir)
    os.environ["SAS_TOKEN"] = "mock_token"

    # mock para ignorar a execução do azcopy + simula sucesso no azcopy
    with patch('taskDownloader.azcopyDir') as mock_azcopyDir:
        mock_azcopyDir.return_value = None
        
        result = simple_download(prefix, year, month, uf, data_group, fake_download_function)

        assert result == "SUCCESS", "O download deveria ter sido bem-sucedido"
        
        expected_download_dir = f"./{prefix}/{data_group}/{year}/{month}/{uf}"
        assert os.path.isdir(expected_download_dir), f"Diretório de download esperado: {expected_download_dir}"
        
        expected_file = os.path.join(expected_download_dir, 'test_file.csv')
        assert os.path.isfile(expected_file), f"O arquivo de download {expected_file} deveria existir"