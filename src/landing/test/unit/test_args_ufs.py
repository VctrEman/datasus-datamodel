from unittest.mock import patch, call
import taskSIH

def teste_ufs_processamento_detalhado():
    # Definindo parâmetros para o teste
    ufs = ["CE", "SP"]
    years = [2021]
    months = [1]
    prefix = "SIH_test"


    with patch('taskSIH.download_data_parallel') as mock_download_data_parallel:
        taskSIH.taskDownloadFile(prefix, years, months, ufs)

        # Verificar as chamadas feitas à função de download
        chamadas = mock_download_data_parallel.call_args_list

        assert len(chamadas) == 1, f"Esperava 1 chamada ao download_data_parallel, mas {len(chamadas)} foram feitas."

        args_passados = chamadas[0][1] 

        assert args_passados['ufs'] == ufs, f"As UFs passadas foram {args_passados['ufs']}, mas esperava {ufs}"
