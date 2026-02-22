from prefect import flow, task
from prefect.logging import get_run_logger
import requests
import re

@task
def load():
    logger=get_run_logger()
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

@flow
def pipeline():
    logger=get_run_logger()
    logger.info("Flow iniciando")
    load_data=load()
    logger.info("Flow terminando")
