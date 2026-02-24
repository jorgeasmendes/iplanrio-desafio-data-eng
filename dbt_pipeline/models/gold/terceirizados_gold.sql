{{ config(
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_id_terc ON {{ this }} (id_terc);
        CREATE INDEX IF NOT EXISTS idx_mes_carga ON {{ this }} (mes_carga)"
    ]
) }}

SELECT
    id_terc,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS sg_orgao_sup_tabela_ug,
    COALESCE(cd_ug_gestora, 'Não informado') AS cd_ug_gestora,
    COALESCE(nm_ug_tabela_ug, 'Não informado') AS nm_ug_tabela_ug,
    COALESCE(sg_ug_gestora, 'Não informado') AS sg_ug_gestora,
    COALESCE(nr_contrato, 'Não informado') AS nr_contrato,
    COALESCE(nr_cnpj, 'Não informado') AS nr_cnpj,
    COALESCE(nm_razao_social, 'Não informado') AS nm_razao_social,
    COALESCE(nr_cpf, 'Não informado') AS nr_cpf,
    COALESCE(nm_terceirizado, 'Não informado') AS nm_terceirizado,
    COALESCE(nm_categoria_profissional, 'Não informado') AS nm_categoria_profissional,
    COALESCE(nm_escolaridade, 'Não informado') AS nm_escolaridade,
    nr_jornada,
    COALESCE(nm_unidade_prestacao, 'Não informado') AS nm_unidade_prestacao,
    vl_mensal_salario,
    vl_mensal_custo,
    COALESCE(sg_orgao, 'Não informado') AS sg_orgao,
    COALESCE(nm_orgao, 'Não informado') AS nm_orgao,
    COALESCE(cd_orgao_siafi, 'Não informado') AS cd_orgao_siafi,
    COALESCE(cd_orgao_siape, 'Não informado') AS cd_orgao_siape,
    COALESCE(mes_carga_tabela, mes_referencia) AS mes_carga
    
FROM {{ ref('terceirizados_silver') }}
