import os
import argparse
import time
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as f

def parse_args():
    parser = argparse.ArgumentParser(description="Process SIH data.")
    parser.add_argument('--read_partition', type=str, required=True, help='Partition to read')
    return parser.parse_args()

def init_spark():
    findspark.init()
    spark = (
        SparkSession.builder.master("local[*]").appName("toSilver")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.1"
        )#.config("spark.io.compression.zstd.level", 1)
        ).getOrCreate()
    return spark

def set_spark_conf(spark, storage_account_name : str, sp_id : str, sp_secret_value : str, sp_directoryId : str) -> None:
    # Receives job environment variables
    if not storage_account_name or not sp_id or not sp_secret_value or not sp_directoryId:
        raise ValueError("One or more required arguments are null: storage_account_name, sp_id, sp_secret_value, sp_directoryId")
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.conf.set("spark.sql.legacy.charVarcharAsString", True)
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_id) #it was complaining about the sp_id null
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_secret_value)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{sp_directoryId}/oauth2/token")

def process_data(spark, read_path, write_path):
    """
    Function to read and process data from parquet file

    Args:
        spark (SparkSession): PySpark session object
        read_path (String): Data file path
        write_path (String): Output Table name
    """
    schemaSIH = {
        'UF_ZI': StringType(),
        'ANO_CMPT': StringType(),
        'MES_CMPT': StringType(),
        'ESPEC': StringType(),
        'CGC_HOSP': StringType(),
        'N_AIH': StringType(),
        'IDENT': 'char(1)',
        'CEP': StringType(),
        'MUNIC_RES': StringType(),
        'NASC': StringType(),
        'SEXO': 'char(1)',
        'UTI_MES_IN': StringType(),
        'UTI_MES_AN': StringType(),
        'UTI_MES_AL': StringType(),
        'UTI_MES_TO': 'numeric(3)',
        'MARCA_UTI': StringType(),
        'UTI_INT_IN': StringType(),
        'UTI_INT_AN': StringType(),
        'UTI_INT_AL': StringType(),
        'UTI_INT_TO': 'numeric(3)',
        'DIAR_ACOM': 'numeric(3)',
        'QT_DIARIAS': 'numeric(3)',
        'PROC_SOLIC': StringType(),
        'PROC_REA': StringType(),
        'VAL_SH': 'numeric(13,2)',
        'VAL_SP': 'numeric(13,2)',
        'VAL_SADT': StringType(),
        'VAL_RN': StringType(),
        'VAL_ACOMP': StringType(),
        'VAL_ORTP': StringType(),
        'VAL_SANGUE': StringType(),
        'VAL_SADTSR': StringType(),
        'VAL_TRANSP': StringType(),
        'VAL_OBSANG': StringType(),
        'VAL_PED1AC': StringType(),
        'VAL_TOT': 'numeric(14,2)',
        'VAL_UTI': 'numeric(8,2)',
        'US_TOT': 'numeric(8,2)',
        'DT_INTER': 'char(8)',
        'DT_SAIDA': 'char(8)',
        'DIAG_PRINC': StringType(),
        'DIAG_SECUN': StringType(),
        'COBRANCA': StringType(),
        'NATUREZA': StringType(),
        'NAT_JUR': StringType(),
        'GESTAO': 'char(1)',
        'RUBRICA': StringType(),
        'IND_VDRL': 'char(1)',
        'MUNIC_MOV': StringType(),
        'COD_IDADE': 'char(1)',
        'IDADE': 'numeric(2)',
        'DIAS_PERM': 'numeric(5)',
        'MORTE': 'numeric(1)',
        'NACIONAL': StringType(),
        'NUM_PROC': StringType(),
        'CAR_INT': StringType(),
        'TOT_PT_SP': StringType(),
        'CPF_AUT': StringType(),
        'HOMONIMO': 'char(1)',
        'NUM_FILHOS': 'numeric(2)',
        'INSTRU': 'char(1)',
        'CID_NOTIF': StringType(),
        'CONTRACEP1': StringType(),
        'CONTRACEP2': StringType(),
        'GESTRISCO': 'char(1)',
        'INSC_PN': StringType(),
        'SEQ_AIH5': StringType(),
        'CBOR': StringType(),
        'CNAER': StringType(),
        'VINCPREV': 'char(1)',
        'GESTOR_COD': StringType(),
        'GESTOR_TP': 'char(1)',
        'GESTOR_CPF': StringType(),
        'GESTOR_DT': StringType(),
        'CNES': StringType(),
        'CNPJ_MANT': StringType(),
        'INFEHOSP': 'char(1)',
        'CID_ASSO': StringType(),
        'CID_MORTE': StringType(),
        'COMPLEX': StringType(),
        'FINANC': StringType(),
        'FAEC_TP': StringType(),
        'REGCT': StringType(),
        'RACA_COR': StringType(),
        'ETNIA': StringType(),
        'SEQUENCIA': 'numeric(9)',
        'REMESSA': StringType(),
        'AUD_JUST': StringType(),
        'SIS_JUST': StringType(),
        'VAL_SH_FED': 'numeric(8,2)',
        'VAL_SP_FED': 'numeric(8,2)',
        'VAL_SH_GES': 'numeric(8,2)',
        'VAL_SP_GES': 'numeric(8,2)',
        'VAL_UCI': 'numeric(8,2)',
        'MARCA_UCI': StringType(),
        'DIAGSEC1': StringType(),
        'DIAGSEC2': StringType(),
        'DIAGSEC3': StringType(),
        'DIAGSEC4': StringType(),
        'DIAGSEC5': StringType(),
        'DIAGSEC6': StringType(),
        'DIAGSEC7': StringType(),
        'DIAGSEC8': StringType(),
        'DIAGSEC9': StringType(),
        'TPDISEC1': 'char(1)',
        'TPDISEC2': 'char(1)',
        'TPDISEC3': 'char(1)',
        'TPDISEC4': 'char(1)',
        'TPDISEC5': 'char(1)',
        'TPDISEC6': 'char(1)',
        'TPDISEC7': 'char(1)',
        'TPDISEC8': 'char(1)',
        'TPDISEC9': 'char(1)'
    }

    print(f"read_path: {read_path}")

    df = spark.read.format('parquet').load(read_path)
    print("Check if the reading schema has the same number of columns of the expected schema, len(expected) - len(read): ", len(schemaSIH) - len(df.columns))

    # Cast columns to specified types
    df = df.select(
        [f.col(column).cast(schemaSIH[column]) for column in df.columns if column in schemaSIH]
    )

    # Trim whitespace and replace empty strings or 0x00 with null, if dtype is StringType()
    df = df.select(
        [
            f.when(
                (f.trim(f.col(column)) == "") |
                (f.trim(f.col(column)) == "0" * f.length(f.col(column))),
                None
            ).otherwise(f.trim(f.col(column))).alias(column)
            if isinstance(df.schema[column].dataType, StringType) 
            else f.col(column)
            for column in df.columns
        ]
    )

    print("Cols written", len(df.columns))
    df.write.option("compression", "none").parquet(write_path, mode="overwrite")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input file to parse", type=str,
                        default="SIH/SIH_JOB/2018/10/*/*")
    parser.add_argument("-o", "--output", help="result file to write", type=str,
                        default="SIH/2018/10")
    args = parser.parse_args()

    start_time = time.time()

    #read_prefix = "SIH/SIH_JOB/2018/*/*/*"
    #write_partition = f"SIH/2018"
    read_file_system = "landing"
    write_file_system = "silver"

    storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    sp_id = os.getenv('sp_id')
    sp_secret_value = os.getenv('sp_secret_value')
    sp_directoryId = os.getenv('sp_directoryId')

    read_path   = f"abfss://{read_file_system}@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/{args.input}"
    write_path  = f"abfss://{write_file_system}@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/{args.output}"
    
    spark = init_spark()
    print(f"storage_account_name\n{storage_account_name}, \nsp_id{sp_id}, znsp_secret_value\n{sp_secret_value}, \nsp_directoryId\n{sp_directoryId}")
    set_spark_conf(spark, 
                    storage_account_name = storage_account_name,
                    sp_id = sp_id,
                    sp_secret_value = sp_secret_value,
                    sp_directoryId = sp_directoryId
                    )

    print("texec: ", time.time() - start_time)
    process_data(spark, read_path, write_path)

    print("texec: ", time.time() - start_time)
    print("success")
