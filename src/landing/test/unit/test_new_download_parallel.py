import time
from concurrent.futures import ProcessPoolExecutor

def dummy_download_function(prefix, year, month, uf):
    time.sleep(1)  # Simulando um tempo de execução
    return f"Downloaded: {prefix}_{year}_{month}_{uf}"

def collect_download_function(prefix, year, month, uf):
    result = dummy_download_function(prefix, year, month, uf)
    return result

def test_download_data_parallel():
    tasks = [
        ("data", 2020, 1, "CE"),
        ("data", 2021, 2, "SP"),
        ("data", 2022, 3, "RJ"),
    ]

    results = []

    def capture_results(future):
        results.append(future.result())

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(collect_download_function, *task) for task in tasks]
        for future in futures:
            capture_results(future)

    expected_results = [
        "Downloaded: data_2020_1_CE",
        "Downloaded: data_2021_2_SP",
        "Downloaded: data_2022_3_RJ"
    ]

    assert results == expected_results, f"Os downloads não foram concluídos corretamente: {results}"