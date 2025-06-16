import os
import polars as pl
from tqdm import tqdm
import concurrent.futures
import re
from pathlib import Path

def parse_sih_metadata_optimized(metadata_text):
    """
    Analisa os metadados para escolher o TIPO DE DADO OTIMIZADO (menor possível).
    Ex: Usa Int16 em vez de Int64 para colunas com números pequenos.
    """
    schema = {}
    lines = metadata_text.strip().split('\n')
    for line in lines:
        parts = line.split(maxsplit=2)
        if len(parts) < 3:
            continue
        
        col_name = parts[1]
        type_str_full = parts[2].split('"')[0].strip()
        
        target_type = None
        if type_str_full.startswith('char'):
            target_type = pl.Utf8
        elif type_str_full.startswith('numeric'):
            match = re.match(r'numeric\((\d+)(,\s*\d+)?\)', type_str_full)
            if match:
                precision = int(match.group(1))
                is_float = bool(match.group(2))

                if is_float:
                    target_type = pl.Float32 
                else:
                    if precision <= 2:
                        target_type = pl.Int8
                    elif precision <= 4:
                        target_type = pl.Int16
                    elif precision <= 9:
                        target_type = pl.Int32
                    else:
                        target_type = pl.Int64
            else:
                target_type = pl.Int64
        
        if target_type:
            schema[col_name] = target_type
            
    return schema

def process_single_file(args):
    """
    Processa um único arquivo: limpa, otimiza os tipos e comprime com Zstandard.
    """
    src_path, dest_path, schema_map = args
    
    try:
        df = pl.read_parquet(src_path)
        original_cols = df.columns
        expressions = []

        # --- Otimização de Tipos e Limpeza ---
        for col_name, target_type in schema_map.items():
            if col_name not in original_cols:
                continue

            expression = pl.col(col_name).cast(pl.Utf8).str.strip_chars()
            expression = pl.when(expression == "").then(None).otherwise(expression)

            # Faz o cast para o tipo final
            if target_type != pl.Utf8:
                expression = expression.cast(target_type)
            
            expressions.append(expression.alias(col_name))

        df_transformed = df.with_columns(expressions)

        df_transformed.write_parquet(
            dest_path,
            compression='zstd',
            compression_level=22,
            statistics=True
        )
        
        original_size = os.path.getsize(src_path)
        compressed_size = os.path.getsize(dest_path)
        return (src_path, original_size, compressed_size, None)

    except Exception as e:
        return (src_path, 0, 0, f"{type(e).__name__}: {e}")

def clean_and_compress_directory(root_dir, metadata_text):
    """
    Orquestra o processo de limpeza e otimização de tipos de dados para um diretório inteiro.
    """
    schema_map = parse_sih_metadata_optimized(metadata_text)
    print("--- Estratégia de Otimização ---")
    print("Foco exclusivo na Otimização de Tipos de Dados (usando os menores tipos possíveis).")
    print("-" * 35)

    dest_root = os.path.join(root_dir, 'cleaned_and_optimized')
    os.makedirs(dest_root, exist_ok=True)
    
    tasks = []
    for dirpath, _, filenames in os.walk(root_dir):
        if Path(dirpath).is_relative_to(Path(dest_root)):
            continue
        for f in filenames:
            if f.endswith('.parquet'):
                src_path = os.path.join(dirpath, f)
                rel_path = os.path.relpath(dirpath, root_dir)
                dest_dir = os.path.join(dest_root, rel_path)
                os.makedirs(dest_dir, exist_ok=True)
                dest_path = os.path.join(dest_dir, f)
                tasks.append((src_path, dest_path, schema_map))

    if not tasks:
        print("Nenhum arquivo .parquet encontrado para processar.")
        return

    print(f"Encontrados {len(tasks)} arquivos .parquet. Iniciando o processo de otimização...")

    total_original_size = 0
    total_compressed_size = 0
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_single_file, tasks), total=len(tasks), desc="Otimizando arquivos"))

    print("\n--- Relatório de Processamento ---")
    errors = []
    for src, original, compressed, error in results:
        if error:
            errors.append(f"  - Falha em {os.path.basename(src)}: {error}")
        else:
            total_original_size += original
            total_compressed_size += compressed
    
    if errors:
        print("\nOcorreram erros durante o processamento:")
        for e in errors:
            print(e)
            
    if total_original_size > 0:
        ratio = total_compressed_size / total_original_size
        savings = 1 - ratio
        print("\n" + "="*50)
        print("Resultados da Otimização Final:")
        print(f"Tamanho original total: {total_original_size / 1024**2:.2f} MB")
        print(f"Tamanho otimizado total: {total_compressed_size / 1024**2:.2f} MB")
        print(f"Taxa de compressão final: {ratio:.2%}")
        print(f"Redução de espaço: {savings:.2%}")
        print("="*50)
    else:
        print("Nenhum arquivo foi processado com sucesso.")

if __name__ == "__main__":
    METADATA = """
    SEQ Nome do Campo Tipo e Tamanho Descrição/Comentários
    1 UF_ZI char(6) Municipality of the healthcare provider.
    2 ANO_CMPT char(4) Year of AIH processing (yyyy).
    3 MES_CMPT char(2) Month of AIH processing (mm).
    4 ESPEC char(2) Type of hospital bed specialty.
    5 CGC_HOSP char(14) Hospital's CNPJ (tax identification number).
    6 N_AIH char(13) AIH (Authorization for Hospitalization) number.
    7 IDENT char(1) Identification of the type of AIH.
    8 CEP char(8) Patient's postal code.
    9 MUNIC_RES char(6) Patient's municipality of residence.
    10 NASC char(8) Patient's date of birth (yyyymmdd).
    11 SEXO char(1) Patient’s gender.
    12 UTI_MES_IN numeric(2) ICU days in the month (input).
    13 UTI_MES_AN numeric(2) ICU days in the month (annual).
    14 UTI_MES_AL numeric(2) ICU days in the month (allocation).
    15 UTI_MES_TOnumeric(3) Total ICU days in the month.
    16 MARCA_UTI char(2) Type of ICU used by the patient.
    17 UTI_INT_IN numeric(2) Intermediate care unit days (input).
    18 UTI_INT_AN numeric(2) Intermediate care unit days (annual).
    19 UTI_INT_AL numeric(2) Intermediate care unit days (allocation).
    20 UTI_INT_TOnumeric(3) Total intermediate care unit days.
    21 DIAR_ACOM numeric(3) Quantidade de diárias de acompanhante.
    22 QT_DIARIAS numeric(3) Quantidade de diárias.
    23 PROC_SOLIC char(10) Procedimento solicitado.
    24 PROC_REA char(10) Procedimento realizado.
    25 VAL_SH "numeric(13, 2)" Valor de serviços hospitalares.
    26 VAL_SP "numeric(13, 2)" Valor de serviços profissionais.
    27 VAL_SADT "numeric(13, 2)" Zerado
    28 VAL_RN "numeric(13, 2)" Zerado
    29 VAL_ACOMP "numeric(13, 2)" Zerado
    30 VAL_ORTP "numeric(13, 2)" Zerado
    31 VAL_SANGUE "numeric(13, 2)" Zerado
    32 VAL_SADTSR "numeric(11, 2)" Zerado
    33 VAL_TRANSP "numeric(13, 2)" Zerado
    34 VAL_OBSANG "numeric(11, 2)" Zerado
    35 VAL_PED1AC "numeric(11, 2)" Zerado
    36 VAL_TOT "numeric(14, 2)" Valor total da AIH.
    37 VAL_UTI "numeric(8, 2)" Valor de UTI.
    38 US_TOT "numeric(8, 2)" Valor total em dólar.
    39 DT_INTER char(8) Data de internação no formato aaammdd.
    40 DT_SAIDA char(8) Data de saída no formato aaaammdd.
    41 DIAG_PRINC char(4) Código do diagnóstico principal (CID10).
    42 DIAG_SECUN char(4) Código do diagnostico secundário (CID10). Preenchido com zeros a partir de 201501.
    43 COBRANCA char(2) Motivo de Saída/Permanência
    44 NATUREZA char(2) Natureza jurídica do hospital (com conteúdo até maio/12). Era utilizada a classificação de Regime e Natureza.
    45 NAT_JUR char(4) Natureza jurídica do Estabelecimento conforme a Comissão Nacional de Classificação - CONCLA
    46 GESTAO char(1) Indica o tipo de gestão do hospital.
    47 RUBRICA numeric(5) Zerado
    48 IND_VDRL char(1) Indica exame VDRL.
    49 MUNIC_MOV char(6) Município do Estabelecimento.
    50 COD_IDADE char(1) Unidade de medida da idade.
    51 IDADE numeric(2) Idade.
    52 DIAS_PERM numeric(5) Dias de Permanência.
    53 MORTE numeric(1) Indica Óbito
    54 NACIONAL char(2) Código da nacionalidade do paciente.
    55 NUM_PROC char(4) Zerado
    56 CAR_INT char(2) Caráter da internação.
    57 TOT_PT_SP numeric(6) Zerado
    58 CPF_AUT char(11) Zerado
    59 HOMONIMO char(1) Indicador se o paciente da AIH é homônimo do paciente de outra AIH.
    60 NUM_FILHOS numeric(2) Número de filhos do paciente.
    61 INSTRU char(1) Grau de instrução do paciente.
    62 CID_NOTIF char(4) CID de Notificação.
    63 CONTRACEP1 char(2) Tipo de contraceptivo utilizado.
    64 CONTRACEP2 char(2) Segundo tipo de contraceptivo utilizado.
    65 GESTRISCO char(1) Indicador se é gestante de risco.
    66 INSC_PN char(12) Número da gestante no pré-natal.
    67 SEQ_AIH5 char(3) Sequencial de longa permanência (AIH tipo 5).
    68 CBOR char(3) Ocupação do paciente segundo a Classificação Brasileira de Ocupações – CBO.
    69 CNAER char(3) Código de acidente de trabalho.
    70 VINCPREV char(1) Vínculo com a Previdência.
    71 GESTOR_COD char(3) Motivo de autorização da AIH pelo Gestor.
    72 GESTOR_TP char(1) Tipo de gestor.
    73 GESTOR_CPF char(11) Número do CPF do Gestor.
    74 GESTOR_DT char(8) Data da autorização dada pelo Gestor (aaaammdd).
    75 CNES char(7) Código CNES do hospital.
    76 CNPJ_MANT char(14) CNPJ da mantenedora.
    77 INFEHOSP char(1) Status de infecção hospitalar.
    78 CID_ASSO char(4) CID causa.
    79 CID_MORTE char(4) CID da morte.
    80 COMPLEX char(2) Complexidade.
    81 FINANC char(2) Tipo de financiamento.
    82 FAEC_TP char(6) Subtipo de financiamento FAEC.
    83 REGCT char(4) Regra contratual.
    84 RACA_COR char(4) Raça/Cor do paciente.
    85 ETNIA char(4) Etnia do paciente se raça cor for indígena.
    86 SEQUENCIA numeric(9) Sequencial da AIH na remessa.
    87 REMESSA char(21) Número da remessa.
    88 AUD_JUST char(50) Justificativa do auditor para aceitação da AIH sem o número do Cartão Nacional de Saúde.
    89 SIS_JUST char(50) Justificativa do estabelecimento para aceitação da AIH sem o número do Cartão Nacional de Saúde.
    90 VAL_SH_FED "numeric(8, 2)" Valor do complemento federal de serviços hospitalares. Está incluído no valor total da AIH.
    91 VAL_SP_FED "numeric(8, 2)" Valor do complemento federal de serviços profissionais. Está incluído no valor total da AIH.
    92 VAL_SH_GES "numeric(8, 2)" Valor do complemento do gestor (estadual ou municipal) de serviços hospitalares. Está incluído no valor total da AIH.
    93 VAL_SP_GES "numeric(8, 2)" Valor do complemento do gestor (estadual ou municipal) de serviços profissionais. Está incluído no valor total da AIH.
    94 VAL_UCI "numeric(8, 2)" Valor de UCI.
    95 MARCA_UCI char(2) Tipo de UCI utilizada pelo paciente.
    96 DIAGSEC1 char(4) Diagnóstico secundário1.
    97 DIAGSEC2 char(4) Diagnóstico secundário 2.
    98 DIAGSEC3 char(4) Diagnóstico secundário 3.
    99 DIAGSEC4 char(4) Diagnóstico secundário 4.
    100 DIAGSEC5 char(4) Diagnóstico secundário 5.
    101 DIAGSEC6 char(4) Diagnóstico secundário 6.
    102 DIAGSEC7 char(4) Diagnóstico secundário 7.
    103 DIAGSEC8 char(4) Diagnóstico secundário 8.
    104 DIAGSEC9 char(4) Diagnóstico secundário 9.
    105 TPDISEC1 char(1) Tipo de diagnóstico secundário 1.
    107 TPDISEC2 char(1) Tipo de diagnóstico secundário 2.
    108 TPDISEC3 char(1) Tipo de diagnóstico secundário 3.
    109 TPDISEC4 char(1) Tipo de diagnóstico secundário 4.
    110 TPDISEC5 char(1) Tipo de diagnóstico secundário 5.
    111 TPDISEC6 char(1) Tipo de diagnóstico secundário 6.
    112 TPDISEC7 char(1) Tipo de diagnóstico secundário 7.
    113 TPDISEC8 char(1) Tipo de diagnóstico secundário 8.
    114 TPDISEC9 char(1) Tipo de diagnóstico secundário 9.
    """
    base_dir = '/home/nycolasdiaas/Workspaces/datasus-datamodel/SIH/'
    
    clean_and_compress_directory(root_dir=base_dir, metadata_text=METADATA)
