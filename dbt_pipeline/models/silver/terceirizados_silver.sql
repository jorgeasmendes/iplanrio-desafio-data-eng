{{ config(
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_mes_referencia ON {{ this }} (mes_referencia)"
    ]
) }}

SELECT
    TRY_CAST(id_terc AS INTEGER) AS id_terceirizado,
    nr_cpf AS terceirizado_cpf,
    nm_terceirizado AS terceirizado_nome,
    nm_categoria_profissional AS terceirizado_categoria_profissional,
    nm_escolaridade AS terceirizado_escolaridade,
    ROUND(TRY_CAST(REPLACE(vl_mensal_salario, ',', '.') AS DOUBLE), 2) AS terceirizado_salario,
    ROUND(TRY_CAST(REPLACE(vl_mensal_custo, ',', '.') AS DOUBLE), 2) AS terceirizado_custo,
    TRY_CAST(nr_jornada AS INTEGER) AS jornada_horas,
    nr_cnpj AS empresa_cnpj,
    nm_razao_social AS empresa_razao_social,
    nr_contrato AS contrato_numero,
    sg_orgao_sup_tabela_ug AS orgao_superior_sigla,
    sg_ug_gestora AS unidade_gestora_sigla,
    nm_ug_tabela_ug AS unidade_gestora_nome,
    cd_ug_gestora AS unidade_gestora_codigo,
    sg_orgao AS orgao_sigla,
    nm_orgao AS orgao_nome,
    cd_orgao_siafi AS orgao_codigo_siafi,
    cd_orgao_siape AS orgao_codigo_siape,
    nm_unidade_prestacao AS unidade_prestacao_nome,
    MAKE_DATE(TRY_CAST(Ano_Carga as INTEGER), TRY_CAST(Num_Mes_Carga as INTEGER), 1) AS mes_carga_tabela,
    mes_referencia
FROM {{ ref('terceirizados_bronze') }}
