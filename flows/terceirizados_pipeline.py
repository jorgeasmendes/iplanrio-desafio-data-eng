from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
import requests
import re
import os
import boto3
import duckdb
from typing import List, Literal
from pydantic import BaseModel, Field
import time

#VARIÁVEIS DE AMBIENTE
AWS_ACCESS_KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION=os.environ.get('AWS_REGION')
BUCKET_NAME=os.environ.get('BUCKET_NAME')

#PARÂMETROS DO FLOW
class FlowParameters(BaseModel):
    tasks: List[Literal[
        "Criar bucket", 
        "Carregar dados brutos", 
        "Rodar DBT"]] = Field(
            default=[
            "Criar bucket", 
            "Carregar dados brutos", 
            "Rodar DBT"
            ],
            title="Tasks",
            description="Etapas que devem ser executadas"
    )

    ano_inicio_carga: int = Field(
        default=2019,
        ge=2019,
        title="Ano inicial",
        description="Ano inicial de carga para dados novos (YYYY)"
    )

    mes_inicio_carga: Literal["01", "05", "09"] = Field(
        default="01",
        title="Mês inicial",
        description="Mês inicial da carga para dados novos"
    )

    ano_fim_carga: int = Field(
        default=2050,
        ge=2019,
        title="Ano final",
        description="Ano final da carga para dados novos (YYYY)"
    )

    mes_fim_carga: Literal["01", "05", "09"] = Field(
        default="01",
        title="Mês final",
        description="Mês final da carga para dados novos"
    )

    comando_dbt: Literal["build", "run", "test"] = Field(
        default = "build",
        title = "Comando DBT",
        description = "Comando a ser executado no DBT"
    )

#TASKS E FLOW
@task(name="Criar Bucket S3")
def create_bucket(run: bool):
    """
    Verifica se o bucket S3 configurado já existe na conta AWS.
    Caso não exista, realiza a criação.

    Parâmetros:
        run (bool): Indica se a task deve ser executada.

    Retorno:
        None
    """
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

@task(name="Carregar Dados Brutos")
def load_raw_data(run: bool, ano_inicio_carga: str, mes_inicio_carga: str, 
                    ano_fim_carga: str, mes_fim_carga: str):
    """
    Carrega os dados brutos no bucket S3 com particionamento por mês de carga.

    Etapas:
        1. Identifica arquivos disponíveis no site de fonte.
        2. Filtra os arquivos conforme o período informado nos parâmetros.
        3. Faz download do arquivo.
        4. Lê com DuckDB.
        5. Exporta para o bucket S3 no formato parquet, particionando por mês de carga. 

    Parâmetros:
        run (bool): Indica se a task deve ser executada.
        ano_inicio_carga (str): Ano inicial da carga.
        mes_inicio_carga (str): Mês inicial da carga.
        ano_fim_carga (str): Ano final da carga.
        mes_fim_carga (str): Mês final da carga.

    Retorno:
        None
    """
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
        retry=0
        while retry<5:
            try:
                with requests.get(link, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open("temp_data", "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                logger.info(f"Arquivo {link} baixado com sucesso")
                break
            except Exception as e:
                if retry==4:
                    logger.error(f"Todas as tentativas falharam")
                    raise
                else:
                    logger.warning(f"Falha ao tentar baixar o arquivo {link}: \n{e}\nTentando novamente...")
                    retry+=1 
                    time.sleep(3)

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

@task(name="Rodar DBT")
def dbt_run(run: bool, comando_dbt: str):
    """
    Executa comando DBT localmente, criando camadas bronze, silver e gold.

    Parâmetros:
        run (bool): Indica se a task deve ser executada.
        comando_dbt (str): Comando DBT a ser executado (build, run ou test).

    Retorno:
        None
    """
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

@task(name="Carregar Dados Transformados")
def load_transformed_data(run: bool):
    """
    Carrega os dados transformados no bucket S3.

    Etapas:
        1. Copia cada tabela do banco (camada do pipeline dbt) como um novo arquivo DuckDB.
        2. Envia para S3 cada camada como bancos de dados indpendentes.
        3. Dispara atualização da API via endpoint HTTP.

    Parâmetros:
        run (bool): Indica se a task deve ser executada.

    Retorno:
        None
    """
    if not run:
        return
    logger=get_run_logger()
    logger.info("Iniciando task load_transformed_data...")
    for camada in ["bronze", "silver", "gold"]:
        if os.path.exists('temp.duckdb'):
            os.remove('temp.duckdb')
        with duckdb.connect('temp.duckdb') as con:
            con.execute("ATTACH 'local.duckdb' AS db_origem (READ_ONLY)")
            con.execute(f"CREATE TABLE terceirizados_{camada} AS SELECT * FROM db_origem.terceirizados_{camada}")
            s3 = boto3.client('s3', 
                  region_name=AWS_REGION, 
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
            s3.upload_file("temp.duckdb", BUCKET_NAME, f"terceirizados/{camada}/terceirizados_{camada}.duckdb")
        
        logger.info(f"Tabela terceirizados_{camada} carregada com sucesso")

    if os.path.exists('temp.duckdb'):
            os.remove('temp.duckdb')
    try:
        logger.info("Atualizando dados da api...")
        response = requests.post("http://fast-api:8000/admin/refresh", timeout=60)
        response.raise_for_status()
        logger.info("Atualização da api executada com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao chamar refresh da API: {e}")

    logger.info("Task load_transformed_data finalizada com sucesso")
    return

@flow(
    name="Pipeline terceirizados",
    description="""
    Pipeline responsável por:

    1. Criar bucket S3 (caso necessário)
    2. Carregar dados brutos no bucket
    3. Executar transformações via DBT localmente
    4. Carregar dados transformados no bucket
    """
)
def pipeline(Parameters: FlowParameters = FlowParameters()):
    """
    Orquestra o pipeline completo de ingestão e transformação
    de dados de terceirizados.

    Parâmetros:
        - tasks: Lista de etapas a serem executadas
        - ano_inicio_carga: Ano inicial da carga
        - mes_inicio_carga: Mês inicial da carga
        - ano_fim_carga: Ano final da carga
        - mes_fim_carga: Mês final da carga
        - comando_dbt: Comando DBT a ser executado

    Fluxo:
        create_bucket → load_raw_data → dbt_run → load_transformed_data
    """
    logger=get_run_logger()
    bucket=create_bucket.submit(run = "Criar bucket" in Parameters.tasks)
    new_data=load_raw_data.submit(run = "Carregar dados brutos" in Parameters.tasks, wait_for=[bucket],
                           ano_inicio_carga=str(Parameters.ano_inicio_carga), mes_inicio_carga=Parameters.mes_inicio_carga,
                           ano_fim_carga=str(Parameters.ano_fim_carga), mes_fim_carga=Parameters.mes_fim_carga)
    dbt_result=dbt_run.submit(run = "Rodar DBT" in Parameters.tasks, wait_for=[new_data],
                       comando_dbt=Parameters.comando_dbt)
    duckdb_uploaded=load_transformed_data(run = "Rodar DBT" in Parameters.tasks and Parameters.comando_dbt != "test", wait_for=[dbt_result])
    logger.info("Flow concluído com sucesso")
