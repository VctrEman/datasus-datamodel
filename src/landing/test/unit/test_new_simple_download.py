import os
from unittest.mock import patch
from taskDownloader import simple_download

def test_simple_download_with_mocks():
    # Definição dos parâmetros de teste
    prefix = "/mock/prefix"
    year = 2021
    month = 1
    uf = "CE"
    data_group = "PA"

    # Função de download simulada para testar o comportamento da função
    def fake_download_function(ufs, years, months, groups, data_dir):
        print(f"Simulated download to: {data_dir}")  # Log para depuração

    # Mock para as variáveis de ambiente e funções do sistema de arquivos
    with patch('taskDownloader.os.getenv') as mock_getenv, \
         patch('taskDownloader.os.makedirs') as mock_makedirs, \
         patch('taskDownloader.os.listdir', return_value=['test_file.csv']) as mock_listdir, \
         patch('taskDownloader.azcopyDir') as mock_azcopyDir, \
         patch('taskDownloader.monitor_cpu_usage') as mock_monitor_cpu_usage:

        # Configuração dos valores retornados pelos mocks
        mock_getenv.side_effect = lambda key: "/mock/sink/dir" if key == 'SINK_BASE_DIR' else 'mock_token' if key == 'SAS_TOKEN' else None
        mock_azcopyDir.return_value = None  # Simulando sucesso na execução do azcopyDir
        mock_monitor_cpu_usage.return_value = None  # Simulando a execução de monitoramento de CPU

        # Execução do download usando a função com os mocks configurados
        result = simple_download(prefix, year, month, uf, data_group, fake_download_function)

        # Verificações
        assert result == "SUCCESS", "O download não foi bem-sucedido"

        # Verificar se o diretório foi criado corretamente
        mock_makedirs.assert_called_once_with(f"./{prefix}/{data_group}/{year}/{month}/{uf}", exist_ok=True)
        
        # Verificar se a listagem de arquivos foi feita corretamente no diretório
        mock_listdir.assert_called_once_with(f"./{prefix}/{data_group}/{year}/{month}/{uf}")

        # Verificando se a chamada ao azcopyDir foi feita corretamente
        mock_azcopyDir.assert_called_once_with(
            source=f"./{prefix}/{data_group}/{year}/{month}/{uf}",
            destination=f"/mock/sink/dir/JOB_{data_group}/{year}/{month}/{uf}?mock_token"
        )
