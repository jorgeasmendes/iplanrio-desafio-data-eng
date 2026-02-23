from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
import requests
import re
import os
import boto3
import duckdb
from typing import List, Literal

AWS_ACCESS_KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION=os.environ.get('AWS_REGION')
BUCKET_NAME=os.environ.get('BUCKET_NAME')

@task
def create_bucket(run: bool):
    if not run:
        return
    logger=get_run_logger()
    logger.info(f"Iniciando task create_bucket...")
    logger.info(f"Verificando existência e propriedade do bucket '{BUCKET_NAME}' na AWS S3")
    s3 = boto3.client('s3', 
                  region_name=AWS_REGION, 
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    try:
        if not any(b['Name'] == BUCKET_NAME for b in s3.list_buckets()['Buckets']):
            logger.info(f"Sua conta não possui bucket com nome '{BUCKET_NAME}'. Criando bucket...")
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
            logger.info(f"Bucket '{BUCKET_NAME}' criado com sucesso!")
        else:
            logger.info(f"Bucket '{BUCKET_NAME}' já existe. Não é preciso criar um novo")
    except Exception as e:
        logger.error(f"Erro ao acessar ou criar o bucket: {e}")
        raise
    logger.info(f"Task create_bucket finalizada com sucesso")
    return

@task
def load_raw_data(run: bool, ano_inicio_carga: str, mes_inicio_carga: str, 
                    ano_fim_carga: str, mes_fim_carga: str):
    if not run:
        return
    logger=get_run_logger()
    logger.info("Iniciando task load_raw_data...")
    encodings = ["utf-8", "latin-1", "utf-16"]
    new_data=[]
        
    logger.info("Pesquisando os arquivos disponíveis para carga...")
    url_base_dados="https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"
    response = requests.get(url_base_dados)
    files=re.findall(rf'href=["\']?({url_base_dados}/arquivos/[^\s"\'>]+\.(csv|xlsx))["\']?', response.text)

    filtered_files=[]
    for file in files:
        yearmonth=file[0].replace('maio', '202505').replace('setembro', '202509')
        file=(file[0], file[1], re.search(r'\d{6}', yearmonth).group())
        init_month=int(ano_inicio_carga+mes_inicio_carga)
        end_month=int(ano_fim_carga+mes_fim_carga)
        if int(file[2])>=init_month and int(file[2])<=end_month:
            filtered_files.append(file)
    filtered_files = sorted(filtered_files, key=lambda x: int(x[2]))

    logger.info(f"{len(filtered_files)} arquivos encontrados")

    for file in filtered_files:
        link=file[0]
        filetype=file[1]
        year_month=file[2][:4] + '-' + file[2][4:]
        
        logger.info(f"Baixando arquivo de {year_month}:\n -Tipo do arquivo: {filetype}\n -Link: {link}")
        try:
            with requests.get(link, stream=True) as r:
                r.raise_for_status()
                with open("temp_data", "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            logger.info(f"Arquivo {link} baixado com sucesso")
        except Exception as e:
            logger.warning(f"Falha ao tentar baixar o arquivo {link}: \n{e}")

        logger.info(f"Enviando arquivo de {year_month} para bucket '{BUCKET_NAME}' na AWS S3")
        con = duckdb.connect(":memory:")
        if filetype=="xlsx":
            con.execute("INSTALL excel; LOAD excel;")
        con.execute("INSTALL httpfs; LOAD httpfs;") 
        con.sql(f"""
                    CREATE TABLE new_data (                             
                        id_terc VARCHAR,
                        sg_orgao_sup_tabela_ug VARCHAR,
                        cd_ug_gestora VARCHAR,
                        nm_ug_tabela_ug VARCHAR,
                        sg_ug_gestora VARCHAR,
                        nr_contrato VARCHAR,
                        nr_cnpj VARCHAR,
                        nm_razao_social VARCHAR,
                        nr_cpf VARCHAR,
                        nm_terceirizado VARCHAR,
                        nm_categoria_profissional VARCHAR,
                        nm_escolaridade VARCHAR,
                        nr_jornada VARCHAR,
                        nm_unidade_prestacao VARCHAR,
                        vl_mensal_salario VARCHAR,
                        vl_mensal_custo VARCHAR,
                        Num_Mes_Carga VARCHAR,
                        Mes_Carga VARCHAR,
                        Ano_Carga VARCHAR,
                        sg_orgao VARCHAR,
                        nm_orgao VARCHAR,
                        cd_orgao_siafi VARCHAR,
                        cd_orgao_siape VARCHAR,
                        mes_referencia DATE
                        );
                        """)
        last_error=None
        for encoding in encodings:
            csv_extra_option = f", quote='\"', encoding='{encoding}'" if filetype=="csv" else ""
            try:   
                con.sql(f"""
                    INSERT INTO new_data
                        SELECT 
                            *, 
                            TRY_CAST('{year_month}-01' AS DATE) AS mes_referencia
                        FROM read_{filetype}('temp_data', all_varchar = true {csv_extra_option});
                            """)
                if filetype=="csv":
                    logger.info(f"Encoding reconhecido no csv: {encoding}")
                break
            except Exception as e:
                if filetype=="xlsx":
                    raise e
                logger.warning(f"Falha ao tentar encoding {encoding}: \n{e}")
                last_error=e
        else:
            raise last_error
            
        con.execute(f"""
            COPY new_data
                TO 's3://{BUCKET_NAME}/terceirizados/raw/mes_referencia={year_month}-01/terceirizados.parquet'
                (FORMAT PARQUET, OVERWRITE)
                """)
        con.close()
            
        logger.info(f"Dados de {year_month} carregados com sucesso no bucket")
        new_data.append(year_month)

    logger.info(f"Task load_raw_data finalizada com sucesso\nForam carregados dados dos meses: {new_data}")
    return

@task
def dbt_run(run: bool, comando_dbt: str):
    if not run:
        return
    logger=get_run_logger()
    logger.info(f"Iniciando task dbt_run...\nComando: dbt {comando_dbt}")
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="dbt_pipeline",
            profiles_dir="dbt_pipeline"
        )
    ).invoke([f"{comando_dbt}"])
    logger.info("Task dbt_run finalizada com sucesso")
    return

@task
def load_transformed_data(run: bool):
    if not run:
        return
    logger=get_run_logger()
    logger.info("Iniciando task load_transformed_data...")
    logger.info("Task load_transformed_data finalizada com sucesso")
    return

@flow
def pipeline(tasks: List[Literal["Criar bucket", "Carregar dados brutos", "Rodar DBT"]] = ["Criar bucket", "Carregar dados brutos", "Rodar DBT"],
             ano_inicio_carga: str="2019", mes_inicio_carga: Literal["01", "05", "09"]="01", 
             ano_fim_carga: str="2050", mes_fim_carga: Literal["01", "05", "09"]="01",
             comando_dbt: Literal["build", "run", "test"]="build"   
             ):
    logger=get_run_logger()
    bucket=create_bucket(run = "Criar bucket" in tasks)
    new_data=load_raw_data(run = "Carregar dados brutos" in tasks, wait_for=[bucket],
                           ano_inicio_carga=ano_inicio_carga, mes_inicio_carga=mes_inicio_carga,
                           ano_fim_carga=ano_fim_carga, mes_fim_carga=mes_fim_carga)
    dbt_result=dbt_run(run = "Rodar DBT" in tasks, wait_for=[new_data],
                       comando_dbt=comando_dbt)
    duckdb_uploaded=load_transformed_data(run = "Rodar DBT" in tasks, wait_for=[dbt_result])
    logger.info("Flow concluído com sucesso")
