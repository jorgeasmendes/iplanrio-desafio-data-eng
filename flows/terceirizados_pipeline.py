from prefect import flow, task
from prefect.logging import get_run_logger
import requests
import re
import os
import boto3
from botocore.exceptions import ClientError

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
def load():
    logger=get_run_logger()
    logger.info(f"Iniciando task load_data...")
    url_base="https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"
        
    logger.info("Pesquisando os arquivos disponíveis para carga...")
    response = requests.get(url_base)
    files=re.findall(r'href=["\']?(https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/[^\s"\'>]+\.(csv|xlsx))["\']?', response.text)

    logger.info(f"{len(files)} arquivos encontrados")

    for file in files:
        filetype=file[1]
        link=file[0]
        text=link.replace('maio', '202505').replace('setembro', '202509')
        year_month=re.search(r'\d{6}', text).group()
        year_month=f"{year_month[:4] + '-' + year_month[4:]}"
        logger.info(f"Baixando arquivo {filetype} do mês {year_month}")
        try:
            response=requests.get(link)
            with open(f"{year_month}.{filetype}", "wb") as f:
                f.write(response.content)
            logger.info(f"Arquivo {link} salvo com sucesso")
        except Exception as e:
            logger.warning(f"Falha ao tentar baixar o arquivo {link}: \n{e}")

    logger.info(f"Task load_data finalizada com sucesso")
    return

@flow
def pipeline():
    logger=get_run_logger()
    bucket=create_bucket()
    new_data=load(wait_for=[bucket])
    logger.info("Flow concluído com sucesso")
