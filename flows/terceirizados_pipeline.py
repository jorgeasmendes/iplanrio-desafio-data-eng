from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
import requests
import re
import os
import boto3
import duckdb

AWS_ACCESS_KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION=os.environ.get('AWS_REGION')
BUCKET_NAME=os.environ.get('BUCKET_NAME')

@task
def create_bucket():
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
def load_raw_data():
    logger=get_run_logger()
    logger.info("Iniciando task load_raw_data...")
    encodings = ["utf-8", "latin-1", "utf-16"]
    new_data=[]
    url_base_dados="https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"
        
    logger.info("Pesquisando os arquivos disponíveis para carga...")
    response = requests.get(url_base_dados)
    files=re.findall(rf'href=["\']?({url_base_dados}/arquivos/[^\s"\'>]+\.(csv|xlsx))["\']?', response.text)

    logger.info(f"{len(files)} arquivos encontrados")

    for file in files:
        filetype=file[1]
        link=file[0]
        text=link.replace('maio', '202505').replace('setembro', '202509')
        year_month=re.search(r'\d{6}', text).group()
        year_month=f"{year_month[:4] + '-' + year_month[4:]}"
        
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

    logger.info(f"Task load_raw_data finalizada com sucesso")
    return new_data

@task
def dbt_run():
    logger=get_run_logger()
    logger.info("Iniciando task dbt_job...")
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir="dbt_pipeline",
            profiles_dir="dbt_pipeline"
        )
    ).invoke(["build"])
    logger.info("Task dbt_job finalizada com sucesso")
    return

@flow
def pipeline():
    logger=get_run_logger()
    bucket=create_bucket()
    new_data=load_raw_data(wait_for=[bucket])
    dbt_result=dbt_run(wait_for=[new_data])
    logger.info("Flow concluído com sucesso")
