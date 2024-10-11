from unittest.mock import patch
import taskDownloader


def test_ufs_detail_process():
    ufs = ["CE", "SP"]
    years = [2024]
    months = [1]
    prefix = "SIH_test"
    data_group = "RD"
    
    with patch("utils.download_data_parallel") as mock_download_data_parallel:
        taskDownloader.task_download_files(prefix, years, months, ufs, data_group, download_function=None)
        
        calls = mock_download_data_parallel.call_args_list
        
        
        assert len(calls) == 1, f"Esperava 1 chamada ao download_data_parallel, mas {len(calls)} foram feitas."
        
        past_args = calls[0][0][0]
        
        expected_ufs = set(ufs)
        actual_ufs = {task[0] for task in past_args}

        assert actual_ufs == expected_ufs        
        
        for task in past_args:
            uf, year, month, task_prefix, task_data_group, download_function = task
            assert task_prefix == prefix, f"Prefixo incorreto: esperado {prefix}, mas foi {task_prefix}"
            assert task_data_group == data_group, f"Grupo de dados incorreto: esperado {data_group}, mas foi {task_data_group}"
            assert uf in ufs, f"UF incorreta: esperado uma das UFs {ufs}, mas foi {uf}"
            assert year in years, f"Ano incorreto: esperado um dos anos {years}, mas foi {year}"
            assert month in months, f"Mês incorreto: esperado um dos meses {months}, mas foi {month}"
