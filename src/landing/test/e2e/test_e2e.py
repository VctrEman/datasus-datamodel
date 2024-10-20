import os
import unittest
import logging
from taskDownloader import task_download_files
from utils import change_cache_directory
from pysus.online_data import SIH

class TestSIHDataDownloadEndToEnd(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.prefix_sih = "./SIH"
        cls.data_group_sih = "RD"
        cls.years = [2024]
        cls.months = [1]
        cls.ufs = ["CE"]
        cls.cache_dir = "./pysus_caching"

        logging.basicConfig(level=logging.INFO)
        cls.logger = logging.getLogger(__name__)

        change_cache_directory(cls.cache_dir)

    def test_sih_download_files(self):
        self.logger.info("Iniciando o teste end-to-end para download de arquivos SIH...")

        task_download_files(
            prefix=self.prefix_sih,
            years=self.years,
            months=self.months,
            ufs=self.ufs,
            data_group=self.data_group_sih,
            download_function=SIH.download
        )

        # Verificar se os arquivos foram baixados
        for uf in self.ufs:
            for year in self.years:
                for month in self.months:
                    download_dir = f"./{self.prefix_sih}/{self.data_group_sih}/{year}/{month}/{uf}"
                    self.assertTrue(os.path.exists(download_dir), f"Diretório de download não encontrado: {download_dir}")
                    files = os.listdir(download_dir)
                    self.assertGreater(len(files), 0, f"Nenhum arquivo foi baixado para {download_dir}")

        self.logger.info("Teste end-to-end de download concluído com sucesso!")

if __name__ == "__main__":
    unittest.main()