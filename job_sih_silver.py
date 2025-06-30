import os
import re
import logging
from pathlib import Path
from typing import Dict, Any, List
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv
import yaml
import polars as pl
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class ParquetProcessor:
    """
    Encapsula a lógica para otimizar e processar arquivos Parquet.

    Esta classe lida com a descoberta de arquivos, aplicação de transformações
    de schema, otimização de tipos de dados e compressão, operando de
    forma paralela para máxima eficiência.
    """

    def __init__(self, source_dir: str, dest_dir: str, metadata_config: List[Dict[str, str]]):
        """
        Inicializa o processador.

        Args:
            source_dir: Diretório onde os arquivos Parquet originais estão localizados.
            dest_dir: Diretório onde os arquivos otimizados serão salvos.
            metadata_config: Configuração do schema carregada do arquivo YAML.
        """
        self.source_path = Path(source_dir)
        self.dest_path = Path(dest_dir)
        self.schema_map = self._parse_schema(metadata_config)

        if not self.source_path.is_dir():
            raise FileNotFoundError(f"O diretório de origem não existe: {self.source_path}")

        self.dest_path.mkdir(parents=True, exist_ok=True)
        logging.info(f"Processador inicializado. Lendo de '{self.source_path}', salvando em '{self.dest_path}'.")

    @staticmethod
    def _parse_schema(metadata_config: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Analisa a configuração de metadados (carregada do YAML) e a converte
        para um dicionário de tipos Polars, selecionando os tipos de dados mais eficientes.
        """
        schema = {}
        if not metadata_config:
            logging.warning("A configuração de metadados está vazia. Nenhuma coluna será mapeada.")
            return schema

        for column_spec in metadata_config:
            col_name = column_spec.get('name')
            type_str_full = column_spec.get('type')

            if not col_name or not type_str_full:
                continue

            target_type = None
            if type_str_full.startswith('char'):
                target_type = pl.Utf8
            elif type_str_full.startswith('numeric'):
                match = re.match(r'numeric\((\d+)(,\s*\d+)?\)', type_str_full)
                if match:
                    precision, is_float = int(match.group(1)), bool(match.group(2))
                    if is_float:
                        target_type = pl.Float32
                    else:
                        if precision <= 2: target_type = pl.Int8
                        elif precision <= 4: target_type = pl.Int16
                        elif precision <= 9: target_type = pl.Int32
                        else: target_type = pl.Int64
                else:
                    target_type = pl.Int64
            
            if target_type:
                schema[col_name] = target_type
        
        logging.info(f"{len(schema)} colunas mapeadas a partir do arquivo de metadados.")
        return schema

    @staticmethod
    def _process_worker(src_file: str, dest_file: str, schema_map: Dict[str, Any]):
        """
        Worker que processa um único arquivo. Projetado para ser executado em um processo separado.
        """
        df = pl.read_parquet(src_file)
        expressions = []

        for col_name, target_type in schema_map.items():
            if col_name in df.columns:
                expression = pl.col(col_name).cast(pl.Utf8).str.strip_chars()
                expression = pl.when(expression == "").then(None).otherwise(expression)
                if target_type != pl.Utf8:
                    expression = expression.cast(target_type)
                expressions.append(expression.alias(col_name))

        df_transformed = df.with_columns(expressions)
        
        dest_path_obj = Path(dest_file)
        dest_path_obj.parent.mkdir(parents=True, exist_ok=True)

        df_transformed.write_parquet(
            dest_path_obj,
            compression='zstd',
            compression_level=22,
            statistics=True
        )
        
        original_size = Path(src_file).stat().st_size
        compressed_size = dest_path_obj.stat().st_size
        return src_file, original_size, compressed_size

    def run(self):
        """
        Orquestra o processo de otimização completo, descobrindo os arquivos
        e processando-os em paralelo.
        """
        logging.info("Iniciando a descoberta de arquivos .parquet no diretório de origem...")
        tasks = []
        for src_path in self.source_path.rglob('*.parquet'):
            relative_path = src_path.relative_to(self.source_path)
            dest_path = self.dest_path / relative_path
            
            tasks.append((src_path, dest_path, self.schema_map))

        if not tasks:
            logging.warning("Nenhum arquivo .parquet encontrado para processar.")
            return

        logging.info(f"Encontrados {len(tasks)} arquivos. Iniciando o processamento paralelo...")

        total_original_size = 0
        total_compressed_size = 0
        success_count = 0
        failed_files = []

        with ProcessPoolExecutor() as executor:
            future_to_task = {executor.submit(self._process_worker, *task): task for task in tasks}
            progress_bar = tqdm(as_completed(future_to_task), total=len(tasks), desc="Otimizando arquivos")
            
            for future in progress_bar:
                src_path, _, _ = future_to_task[future]
                try:
                    _, original_size, compressed_size = future.result()
                    total_original_size += original_size
                    total_compressed_size += compressed_size
                    success_count += 1
                except Exception as e:
                    logging.error(f"Falha ao processar o arquivo {src_path.name}: {e}")
                    failed_files.append(src_path.name)

        self._generate_report(total_original_size, total_compressed_size, success_count, failed_files)

    def _generate_report(self, original_size, compressed_size, success_count, failed_files):
        """
        Gera um relatório final conciso no console. Se houver erros, eles serão
        detalhados. Caso contrário, uma mensagem de sucesso é exibida.
        """
        logging.info("Processamento concluído.")

        if failed_files:
            logging.warning(f"O processo terminou com {len(failed_files)} erro(s).")
            print("\n" + "="*50)
            print("        Relatório de Falhas no Processamento")
            print("="*50)
            print("Os seguintes arquivos não puderam ser processados:")
            for f in failed_files[:10]:
                print(f"  - {f}")
            if len(failed_files) > 10:
                print(f"  ... e mais {len(failed_files) - 10} arquivos.")
            print("\nOs arquivos processados com sucesso foram salvos.")
            print("="*50)
        
        elif success_count > 0:
            ratio = compressed_size / original_size
            savings = 1 - ratio
            print("\n" + "="*50)
            print("        ✅ Processo Concluído com Sucesso")
            print("="*50)
            print(f"  - Arquivos otimizados: {success_count}")
            print(f"  - Tamanho original: {original_size / 1024**2:.2f} MB")
            print(f"  - Tamanho final: {compressed_size / 1024**2:.2f} MB")
            print(f"  - Redução de espaço: {savings:.2%}")
            print("="*50)
            
        else:
            logging.warning("Nenhum arquivo foi encontrado e processado.")


def main():
    load_dotenv()

    source_dir = os.getenv("SOURCE_DIRECTORY")
    dest_dir = os.getenv("DESTINATION_DIRECTORY")
    metadata_file = os.getenv("METADATA_FILE_PATH", "metadata.yaml")

    if not source_dir or not dest_dir:
        logging.error("As variáveis de ambiente 'SOURCE_DIRECTORY' e 'DESTINATION_DIRECTORY' devem ser definidas no arquivo .env.")
        return

    try:
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata_config = yaml.safe_load(f)
        logging.info(f"Arquivo de metadados '{metadata_file}' carregado com sucesso.")
    except FileNotFoundError:
        logging.error(f"Erro: O arquivo de metadados '{metadata_file}' não foi encontrado.")
        return
    except yaml.YAMLError as e:
        logging.error(f"Erro ao analisar o arquivo YAML: {e}")
        return

    try:
        processor = ParquetProcessor(
            source_dir=source_dir,
            dest_dir=dest_dir,
            metadata_config=metadata_config.get('columns', [])
        )
        processor.run()
    except FileNotFoundError as e:
        logging.error(e)
    except Exception as e:
        logging.critical(f"Ocorreu um erro inesperado e fatal: {e}", exc_info=True)

if __name__ == "__main__":
    main()
