{{ config(
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_mes_referencia ON {{ this }} (mes_referencia)"
    ]
) }}

SELECT
    TRY_CAST(id_terc AS INTEGER) AS id_terc,
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
    TRY_CAST(nr_jornada AS INTEGER) AS nr_jornada,
    nm_unidade_prestacao,
    ROUND(TRY_CAST(REPLACE(vl_mensal_salario, ',', '.') AS DOUBLE), 2) AS vl_mensal_salario,
    ROUND(TRY_CAST(REPLACE(vl_mensal_custo, ',', '.') AS DOUBLE), 2) AS vl_mensal_custo,
    MAKE_DATE(TRY_CAST(Ano_Carga as INTEGER), TRY_CAST(Num_Mes_Carga as INTEGER), 1) AS mes_carga_tabela,
    sg_orgao,
    nm_orgao,
    cd_orgao_siafi,
    cd_orgao_siape,
    mes_referencia
FROM {{ ref('terceirizados_bronze') }}
