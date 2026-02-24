from fastapi import FastAPI, Query, HTTPException
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

@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(load_data)

@app.post("/admin/refresh", 
          summary="Atualizar os dados",
          description="Baixa novamente os dados da camada gold no bucket S3")
async def refresh_data():
    await asyncio.to_thread(load_data)
    return {"status": "dados carregados"}

@app.get("/")
async def redirect_to_docs():
    return RedirectResponse(url="/redoc")

@app.get("/terceirizados", 
          summary="Listar todos os terceirizados",
          description="""
            Retorna lista paginada de terceirizados.

            - Ordenação: crescente por `id_terc`
            - Máximo de 200 registros por página
            """)
async def get_terceirizados(page_size: int = Query(50, le=200, description="Quantidade de registros por página (máx: 200)"), 
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
            raise HTTPException(status_code=500, detail=f"Erro ao acessar banco de dados: {e}")
                
        return total, data
        
    total, data = await asyncio.to_thread(query)
    if not data:
        raise HTTPException(status_code=404, detail="Não foram encontrados registros")
        
    return {"total_rows": total, "page_size": page_size, 
            "page": page, "total_pages": (total+page_size-1)//page_size, "data": data}

@app.get("/terceirizados/{id}", 
          summary="Detalhes do terceirizado",
          description="Mostra todos os dados referentes ao terceirizado com o id especificado")
async def get_terceirizados_id(id: int):
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
            raise HTTPException(status_code=500, detail=f"Erro ao acessar o banco: {e}")
                
        return data
        
    data = await asyncio.to_thread(query)
    if not data:
        raise HTTPException(status_code=404, detail="Registro não encontrado")
    
    return {"data": data}