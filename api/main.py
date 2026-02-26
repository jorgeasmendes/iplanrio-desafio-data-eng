from fastapi import FastAPI, Query, Path, HTTPException
from fastapi.responses import RedirectResponse
import boto3
import asyncio
import os
import duckdb

AWS_ACCESS_KEY_ID=os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION=os.environ.get('AWS_REGION')
BUCKET_NAME=os.environ.get('BUCKET_NAME')
DB_FILE = "local.duckdb"

app = FastAPI(
    title="Terceirizados do Governo Federal",
    description="API pública somente leitura para consulta de dados de terceirizados do governo federal brasileiro."
)

def load_data():
    """
    Carrega os dados do banco de dados S3 para construir o banco DuckDB local.
    Cria arquivo temporário para que a atualização seja atômica.
    """
    temp_file = "local.duckdb.tmp"

    s3 = boto3.client('s3', 
                  region_name=AWS_REGION, 
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.download_file(BUCKET_NAME, "terceirizados/gold/terceirizados_gold.duckdb", temp_file)

    os.replace(temp_file, DB_FILE)

def create_empty_db():
    """
    Cria um banco de dados vazio. 
    Função chamada como fallback caso o carregamento do dados falhe no startup.
    """
    con=duckdb.connect(DB_FILE)
    con.close()

@app.on_event("startup")
async def startup_event():
    """
    Evento executado na inicialização da aplicação.

    Tenta sincronizar o banco local com o arquivo armazenado no S3.
    Em caso de falha, cria banco vazio como fallback.
    """
    try:
        await asyncio.to_thread(load_data)
    except Exception:
        await asyncio.to_thread(create_empty_db)

@app.post("/admin/refresh", 
          summary="Atualizar os dados",
          description="""
          Atualiza os dados da API
          """,
          responses={
            500: {
                  "description": "Erro ao baixar o arquivo do S3 ou substituir o banco local",
                  "content": {
                      "application/json": {
                          "example": {
                              "detail": "Erro ao atualizar base de dados"
                          }
                      }
                  }
              }
          }
      )
async def refresh_data():
    """
    Carrega novamente os dados armazendo na AWS S3 para atualizar o banco DuckDB local.
    Cria arquivo temporário para que a atualização seja atômica.
    Semelhante ao startup.
    """
    try:
        await asyncio.to_thread(load_data)
        return {"status": "dados carregados"}
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Erro ao atualizar base de dados"
        )

@app.get("/", 
         summary="Redirecionar para essa documentação")
async def redirect_to_docs():
    return RedirectResponse(url="/docs")

@app.get("/terceirizados", 
          summary="Listar todos os terceirizados",
          description="""
            Retorna lista paginada de terceirizados.

            - Ordenação: crescente por `id_terc`
            - Máximo de 200 registros por página
            """,
            responses={
                404: {
                    "description": "Nenhum registro encontrado para os parâmetros informados",
                    "content": {
                        "application/json": {
                            "example": {"detail": "Nenhum registro encontrado para os parâmetros informados"}
                        }
                    }
                },
                422: {
                    "description": "Erro de validação (ex: page_size maior que 200)",
                },
                500: {
                    "description": "Erro interno ao acessar o banco de dados",
                    "content": {
                        "application/json": {
                            "example": {"detail": "Erro interno ao acessar o banco de dados"}
                        }
                    }
                }
            })
async def get_terceirizados(page_size: int = Query(50, ge=1, le=200, description="Quantidade de registros por página (máx: 200)"), 
                            page: int = Query(0, ge=0, description="Número da página (inicia em 0)")):
    """
    Retorna uma lista paginada dos funcionários terceirizados.

    Parâmetros:
        page_size (int): Número de registros retornados por página. Máximo permitido: 200.
        page (int): Número da página a ser retornada (começa em 0).

    Retorno:
        dict: Contém os seguintes campos:
            - total_rows (int): Total de registros na base de dados.
            - page_size (int): Quantidade de registros retornados nesta página.
            - page (int): Número da página atual.
            - total_pages (int): Total de páginas disponíveis.
            - data (list): Lista de dicionários com os dados dos terceirizados.

    Exceções:
        HTTPException 404: Nenhum registro encontrado para os parâmetros informados.
        HTTPException 422: Erro de validação (ex: page_size maior que 200).
        HTTPException 500: Erro interno ao acessar o banco de dados.
    """
    def query():
        try:
            with duckdb.connect(DB_FILE) as con:
                total = con.sql("SELECT COUNT(*) FROM terceirizados_gold").fetchone()[0]
                result = con.sql("""SELECT 
                                    id_terceirizado, 
                                    terceirizado_cpf,
                                    orgao_superior_sigla,
                                    empresa_cnpj
                                    FROM terceirizados_gold
                                    ORDER BY id_terceirizado ASC
                                    LIMIT ? OFFSET ?""",
                                    params=[page_size, page * page_size]).fetchall()
                
                columns = [
                    "id_terceirizado",
                    "terceirizado_cpf",
                    "orgao_superior_sigla",
                    "empresa_cnpj"
                ]
                
                data = [dict(zip(columns, row)) for row in result] if result else []

        except Exception as e:
            raise HTTPException(status_code=500, detail="Erro interno ao acessar o banco de dados")
                
        return total, data
        
    total, data = await asyncio.to_thread(query)
    if not data:
        raise HTTPException(status_code=404, detail="Nenhum registro encontrado para os parâmetros informados")
        
    return {"total_rows": total, "page_size": page_size, 
            "page": page, "total_pages": (total+page_size-1)//page_size, "data": data}

@app.get("/terceirizados/{id}", 
          summary="Detalhes do terceirizado",
          description="Mostra todos os dados referentes ao terceirizado com o id especificado",
            responses={
                404: {
                    "description": "Registro não encontrado para o ID informado",
                    "content": {
                        "application/json": {
                            "example": {"detail": "Registro não encontrado para o ID informado"}
                        }
                    }
                },
                422: {
                    "description": "Erro de validação (ex: ID inválido ou negativo)",
                },
                500: {
                    "description": "Erro interno ao acessar o banco de dados",
                    "content": {
                        "application/json": {
                            "example": {"detail": "Erro interno ao acessar o banco de dados"}
                        }
                    }
                }
            }
        )
async def get_terceirizados_id(id: int = Path(..., description="ID do terceirizado a ser consultado")):
    """
    Retorna todos os dados disponíveis para um terceirizado específico.

    Parâmetros:
        id (int): ID do terceirizado a ser consultado.

    Retorno:
        dict: Contém o campo:
            - data (dict): Dicionário com todos os campos referentes ao terceirizado e sua contratação.

    Exceções:
        HTTPException 404: Registro não encontrado para o ID informado.
        HTTPException 422: Erro de validação (ex: ID inválido ou negativo).
        HTTPException 500: Erro interno ao acessar o banco de dados.
    """
    def query():
        try:
            with duckdb.connect(DB_FILE) as con:
                result = con.sql("""SELECT 
                                    id_terceirizado,
                                    terceirizado_cpf,
                                    terceirizado_nome,
                                    terceirizado_categoria_profissional,
                                    terceirizado_escolaridade,
                                    terceirizado_salario,
                                    terceirizado_custo,
                                    jornada_horas,
                                    empresa_cnpj,
                                    empresa_razao_social,
                                    contrato_numero,
                                    orgao_superior_sigla,
                                    unidade_gestora_sigla,
                                    unidade_gestora_nome,
                                    unidade_gestora_codigo,
                                    orgao_sigla,
                                    orgao_nome,
                                    orgao_codigo_siafi,
                                    orgao_codigo_siape,
                                    unidade_prestacao_nome,
                                    mes_carga
                                    FROM terceirizados_gold
                                    WHERE id_terceirizado = ?
                                    """,
                                    params=[id]).fetchone()
                if not result:
                    return None
                
                columns = [
                    "id_terceirizado",
                    "terceirizado_cpf",
                    "terceirizado_nome",
                    "terceirizado_categoria_profissional",
                    "terceirizado_escolaridade",
                    "terceirizado_salario",
                    "terceirizado_custo",
                    "jornada_horas",
                    "empresa_cnpj",
                    "empresa_razao_social",
                    "contrato_numero",
                    "orgao_superior_sigla",
                    "unidade_gestora_sigla",
                    "unidade_gestora_nome",
                    "unidade_gestora_codigo",
                    "orgao_sigla",
                    "orgao_nome",
                    "orgao_codigo_siafi",
                    "orgao_codigo_siape",
                    "unidade_prestacao_nome",
                    "mes_carga"
                ]
                
                data = dict(zip(columns, result))
        except Exception as e:
            raise HTTPException(status_code=500, detail="Erro interno ao acessar o banco de dados")
                
        return data
        
    data = await asyncio.to_thread(query)
    if not data:
        raise HTTPException(status_code=404, detail="Registro não encontrado para o ID informado")
    
    return {"data": data}