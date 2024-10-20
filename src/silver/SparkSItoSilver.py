import os
import argparse
import time
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DecimalType
import pyspark.sql.functions as f
from utils_spark import get_schema

def init_spark():
    findspark.init()
    jar_list = [
        "com.fasterxml.jackson.core_jackson-core-2.10.5.jar",
        "com.google.code.findbugs_jsr305-3.0.2.jar",
        "com.google.errorprone_error_prone_annotations-2.2.0.jar",
        "com.google.guava_failureaccess-1.0.jar",
        "com.google.guava_guava-27.0-jre.jar",
        "com.google.guava_listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar",
        "com.google.j2objc_j2objc-annotations-1.1.jar",
        "com.microsoft.azure_azure-keyvault-core-1.0.0.jar",
        "com.microsoft.azure_azure-storage-7.0.1.jar",
        "commons-codec_commons-codec-1.11.jar",
        "commons-logging_commons-logging-1.1.3.jar",
        "org.apache.hadoop.thirdparty_hadoop-shaded-guava-1.1.1.jar",
        "org.apache.hadoop_hadoop-azure-3.3.1.jar",
        "org.apache.httpcomponents_httpclient-4.5.13.jar",
        "org.apache.httpcomponents_httpcore-4.4.13.jar",
        "org.checkerframework_checker-qual-2.5.2.jar",
        "org.codehaus.jackson_jackson-core-asl-1.9.13.jar",
        "org.codehaus.jackson_jackson-mapper-asl-1.9.13.jar",
        "org.codehaus.mojo_animal-sniffer-annotations-1.17.jar",
        "org.eclipse.jetty_jetty-util-9.4.40.v20210413.jar",
        "org.eclipse.jetty_jetty-util-ajax-9.4.40.v20210413.jar",
        "org.slf4j_slf4j-api-1.7.30.jar",
        "org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
    ]
    spark_home_jars = os.getenv('SPARK_HOME') + "/jars/"
    missing_files = [jar for jar in jar_list if not os.path.isfile(spark_home_jars + jar)]
    if missing_files:
        print(f"Warning: The following JAR files are missing: {missing_files}")
    else:
        print("All JAR files found.")
    jars_concatenated = ",".join([spark_home_jars + jar for jar in jar_list])

    spark = (
        SparkSession.builder.master("local[*]").appName("toSilver")
        .config("spark.jars", jars_concatenated)
        .config("spark.io.compression.zstd.level", "3")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.executor.memory", "4g")  # Allocate half of the memory to Spark executors
        .config("spark.driver.memory", "2g")    # Allocate part of the memory to Spark driver
        .config("spark.sql.shuffle.partitions", "2")  # Number of shuffle partitions (equal to vCPUs)
        .config("spark.executor.cores", "1")    # 1 core per executor
        .config("spark.driver.cores", "1")      # Use 1 core for the driver
        .config("spark.default.parallelism", "2") # Adjust parallelism to match the number of vCPUs
        ).getOrCreate()
    return spark

def set_spark_conf(spark, storage_account_name : str, sp_id : str, sp_secret_value : str, sp_directoryId : str) -> None:
    # Receives job environment variables
    if not storage_account_name or not sp_id or not sp_secret_value or not sp_directoryId:
        print("storage account name returned: ",os.getenv('STORAGE_ACCOUNT_NAME'))
        raise ValueError("One or more required arguments are null: storage_account_name, sp_id, sp_secret_value, sp_directoryId")
    spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
    spark.conf.set("spark.sql.legacy.charVarcharAsString", True)
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_id) #it was complaining about the sp_id null
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_secret_value)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{sp_directoryId}/oauth2/token")
    spark.sparkContext.setLogLevel("ERROR")

def process_data(spark, read_path : str, write_path : str, schema_name : str) -> None:
    """
    Function to read and process data from parquet file

    Args:
        spark (SparkSession): PySpark session object
        read_path (String): Data file path
        write_path (String): Output Table name
        schema_name (String): Schema name to be used from utils
    """
    #expected schema
    schemaWrite, schemaRead = get_schema(schema_name)

    print(f"read_path: {read_path}")

    df = spark.read.schema(schemaRead).format('parquet').load(read_path)
    print("Check if the reading schema has the same number of columns of the expected schema, len(expected) - len(read): ", len(schemaWrite) - len(df.columns))

    # Cast columns to specified types
    df = df.select(
        [f.col(column).cast(schemaWrite[column]) for column in df.columns if column in schemaWrite]
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
    df.write.option("compression", "zstd").parquet(write_path, mode="overwrite")

if __name__ == "__main__":
    #it can be used both for SIA and SIH
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input file to parse", type=str,
                        default="SIH/SIH_JOB/2018/10/*/*")
    parser.add_argument("-o", "--output", help="result file to write", type=str,
                        default="TEST/SILVER/SIH/2018/10")
    parser.add_argument( "--source_bucket", help="container, bucket where data will be read from", type=str,
                        default="landing")
    parser.add_argument( "--sink_bucket", help="container, bucket where data will be written to", type=str,
                        default="sandbox")
    parser.add_argument( "--schema_name", help="schema_name", type=str,
                        default="SIH")
    args = parser.parse_args()

    print("Starting job...")

    start_time = time.time()

    storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    sp_id = os.getenv('sp_id')
    sp_secret_value = os.getenv('sp_secret_value')
    sp_directoryId = os.getenv('sp_directoryId')

    read_path   = f"abfss://{args.source_bucket}@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/{args.input}"
    write_path  = f"abfss://{args.sink_bucket}@{os.getenv('STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net/{args.output}"
    
    spark = init_spark()

    set_spark_conf(spark, 
                    storage_account_name = storage_account_name,
                    sp_id = sp_id,
                    sp_secret_value = sp_secret_value,
                    sp_directoryId = sp_directoryId
                    )
 
    print("checkpoint texec: ", time.time() - start_time)
    process_data(spark, read_path, write_path, args.schema_name)

    print("total texec: ", time.time() - start_time)
    print("SUCCESS: PySpark job executed successfully.")
    spark.stop()