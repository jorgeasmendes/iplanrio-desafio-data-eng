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
    title="API de consulta a dados de terceirizados do governo federal brasileiro",
    description="API pública somente leitura para consulta de terceirizados."
)

def load_data():
    temp_file = "local.duckdb.tmp"

    s3 = boto3.client('s3', 
                  region_name=AWS_REGION, 
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.download_file(BUCKET_NAME, "terceirizados/gold/terceirizados_gold.duckdb", temp_file)

    os.replace(temp_file, DB_FILE)

def create_empty_db():
    con=duckdb.connect(DB_FILE)
    con.close()

@app.on_event("startup")
async def startup_event():
    try:
        await asyncio.to_thread(load_data)
    except Exception:
        await asyncio.to_thread(create_empty_db)

@app.post("/admin/refresh", 
          summary="Atualizar os dados",
          description="Baixa novamente os dados da camada gold no bucket S3",
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
    return RedirectResponse(url="/redoc")

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
    def query():
        try:
            with duckdb.connect(DB_FILE) as con:
                total = con.sql("SELECT COUNT(*) FROM terceirizados_gold").fetchone()[0]
                result = con.sql("""SELECT 
                                    id_terc, 
                                    sg_orgao_sup_tabela_ug,
                                    nr_cnpj, 
                                    nr_cpf
                                    FROM terceirizados_gold
                                    ORDER BY id_terc ASC
                                    LIMIT ? OFFSET ?""",
                                    params=[page_size, page * page_size]).fetchall()
                
                columns = ["id_terc", 
                            "sg_orgao_sup_tabela_ug",
                            "nr_cnpj", 
                            "nr_cpf"]
                
                data = [dict(zip(columns, row)) for row in result] if result else []

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro interno ao acessar o banco de dados")
                
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
    def query():
        try:
            with duckdb.connect(DB_FILE) as con:
                result = con.sql("""SELECT 
                                    id_terc,
                                    sg_orgao_sup_tabela_ug,
                                    cd_ug_gestora,
                                    nm_ug_tabela_ug,
                                    sg_ug_gestora,
                                    nr_contrato,
                                    nr_cnpj,
                                    nm_razao_social,
                                    nr_cpf,
                                    nm_terceirizado,
                                    nm_categoria_profissional,
                                    nm_escolaridade,
                                    nr_jornada,
                                    nm_unidade_prestacao,
                                    vl_mensal_salario,
                                    vl_mensal_custo,
                                    sg_orgao,
                                    nm_orgao,
                                    cd_orgao_siafi,
                                    cd_orgao_siape,
                                    mes_carga
                                    FROM terceirizados_gold
                                    WHERE id_terc = ?
                                    """,
                                    params=[id]).fetchone()
                if not result:
                    return None
                
                columns = [
                    "id_terc",
                    "sg_orgao_sup_tabela_ug",
                    "cd_ug_gestora",
                    "nm_ug_tabela_ug",
                    "sg_ug_gestora",
                    "nr_contrato",
                    "nr_cnpj",
                    "nm_razao_social",
                    "nr_cpf",
                    "nm_terceirizado",
                    "nm_categoria_profissional",
                    "nm_escolaridade",
                    "nr_jornada",
                    "nm_unidade_prestacao",
                    "vl_mensal_salario",
                    "vl_mensal_custo",
                    "sg_orgao",
                    "nm_orgao",
                    "cd_orgao_siafi",
                    "cd_orgao_siape",
                    "mes_carga"
                    ]
                
                data = dict(zip(columns, result))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro interno ao acessar o banco de dados")
                
        return data
        
    data = await asyncio.to_thread(query)
    if not data:
        raise HTTPException(status_code=404, detail="Registro não encontrado para o ID informado")
    
    return {"data": data}