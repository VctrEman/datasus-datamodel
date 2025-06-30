import os
import shutil
import json
import time
from pathlib import Path
from datetime import datetime
from kaggle.api.kaggle_api_extended import KaggleApi

def configure_kaggle_api():
    """Autentica e retorna uma instância da API do Kaggle."""
    print("Autenticando com a API do Kaggle...")
    api = KaggleApi()
    api.authenticate()
    return api

def copy_local_files(source_dir, dest_dir):
    """Copia o conteúdo de um diretório de origem para um de destino."""
    source_path = Path(source_dir)
    if not source_path.is_dir():
        raise FileNotFoundError(f"O diretório de origem não foi encontrado em: '{source_dir}'")
        
    print(f"Copiando arquivos de '{source_dir}' para '{dest_dir}'...")
    
    shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
    
    files_in_dest = sum(len(files) for _, _, files in os.walk(dest_dir))
    if files_in_dest == 0:
         raise FileNotFoundError(f"Nenhum arquivo foi copiado de '{source_dir}'. Verifique se o diretório não está vazio.")

    print(f"Cópia concluída. {files_in_dest} arquivos prontos para o upload.")
    return True

def create_metadata_file(dataset_id, local_dir):
    """Cria o arquivo dataset-metadata.json necessário para a atualização."""
    metadata = {
        "id": dataset_id,
        "title": dataset_id.split('/')[1],
        "licenses": [{"name": "CC0-1.0"}]
    }
    metadata_path = Path(local_dir) / 'dataset-metadata.json'
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=4)
        
    print(f"Arquivo de metadados 'dataset-metadata.json' criado em '{local_dir}'.")

def update_kaggle_dataset(api, dataset_id, local_dir, version_notes):
    """
    Atualiza um dataset existente no Kaggle com o conteúdo de um diretório local.
    """
    print(f"\nIniciando atualização do dataset '{dataset_id}' no Kaggle...")
    print(f"Conteúdo do diretório a ser enviado '{local_dir}':")
    paths_to_show = list(Path(local_dir).rglob('*'))[:15]
    for path in paths_to_show:
        if path.is_file():
            print(f"  - {path.relative_to(local_dir)}")
    if len(paths_to_show) < sum(len(files) for _, _, files in os.walk(local_dir)):
        print("  - ... e mais")


    try:
        api.dataset_create_version(
            folder=local_dir,
            version_notes=version_notes,
            dir_mode='zip' 
        )
        print("\n✅ Sucesso! Nova versão do dataset enviada para o Kaggle.")
        print("Aguardando o processamento do Kaggle para confirmar a atualização...")
        return True
        
    except Exception as e:
        print(f"\n❌ Falha ao atualizar o dataset no Kaggle: {e}")
        return False

def check_upload_status(api, dataset_id, max_wait_minutes=15):
    """
    Verifica o status do upload no Kaggle em tempo real após o envio.
    """
    print("\nVerificando o status do processamento no Kaggle em tempo real...")
    start_time = time.time()
    wait_seconds = 30
    
    while time.time() - start_time < max_wait_minutes * 60:
        try:
            status_result = api.dataset_status(dataset_id)
            if isinstance(status_result, dict):
                status = status_result.get('status')
            elif isinstance(status_result, str):
                status = status_result
            else:
                status = 'unknown'

            print(f"  - {datetime.now().strftime('%H:%M:%S')}: Status atual é '{status}'.")

            if status == 'ready':
                print("\n🎉 Processamento concluído! A nova versão do dataset está disponível no Kaggle.")
                return
            elif status == 'error':
                error_message = status_result.get('message', 'Erro desconhecido.') if isinstance(status_result, dict) else status_result
                print(f"\n❌ Erro durante o processamento no Kaggle: {error_message}")
                return
            
            time.sleep(wait_seconds)
            
        except Exception as e:
            print(f"\nOcorreu um erro ao verificar o status: {e}")
            print("Pode ser necessário verificar o status manualmente no site do Kaggle.")
            return
            
    print(f"\nTempo de espera de {max_wait_minutes} minutos excedido. O processamento pode estar demorando mais que o esperado.")
    print("Por favor, verifique o status manualmente no site do Kaggle.")


def main():
    """Função principal para orquestrar o processo de upload."""
    
    LOCAL_SOURCE_DIR = '/home/nycolasdiaas/Workspaces/datasus-datamodel/SIH/RD/cleaned_and_optimized2/'
    KAGGLE_DATASET_ID = 'victoremanuel/bigsus-sih-silver' 
    LOCAL_UPLOAD_DIR = './kaggle_upload_temp'

    api = None
    upload_succeeded = False
    try:
        api = configure_kaggle_api()

        if os.path.exists(LOCAL_UPLOAD_DIR):
            shutil.rmtree(LOCAL_UPLOAD_DIR)
            print(f"\nDiretório temporário antigo '{LOCAL_UPLOAD_DIR}' removido.")
        os.makedirs(LOCAL_UPLOAD_DIR, exist_ok=True)

        copy_local_files(LOCAL_SOURCE_DIR, LOCAL_UPLOAD_DIR)

        create_metadata_file(KAGGLE_DATASET_ID, LOCAL_UPLOAD_DIR)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        version_notes = f"Atualização automática de dados do diretório local em {timestamp}"
        
        upload_succeeded = update_kaggle_dataset(api, KAGGLE_DATASET_ID, LOCAL_UPLOAD_DIR, version_notes)
        
        if upload_succeeded:
            check_upload_status(api, KAGGLE_DATASET_ID)

    except Exception as e:
        print(f"\nOcorreu um erro geral no processo: {e}")
    finally:
        if os.path.exists(LOCAL_UPLOAD_DIR):
            try:
                shutil.rmtree(LOCAL_UPLOAD_DIR)
                print(f"\nDiretório temporário '{LOCAL_UPLOAD_DIR}' limpo com sucesso.")
            except Exception as e:
                print(f"Falha ao limpar o diretório temporário: {e}")

if __name__ == "__main__":
    main()
