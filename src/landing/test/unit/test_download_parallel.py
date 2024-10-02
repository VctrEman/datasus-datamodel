def download_data_parallel(prefix: str, ufs: list, years: list, months: list, downloadFun) -> None:
    for uf in ufs:
        for year in years:
            for month in months:
                result = downloadFun(prefix, year, month, uf)
                if result is None:  # Simulando erro
                    return False
    return True


def test_download_data_parallel():
    
    def fake_download_fun(prefix, year, month, uf):
        return True  # download bem-sucedido

    prefix = "SIA"
    ufs = ["SP", "RJ"]
    years = [2023]
    months = [1, 2]

    result = download_data_parallel(prefix, ufs, years, months, fake_download_fun)
    
    assert result == True