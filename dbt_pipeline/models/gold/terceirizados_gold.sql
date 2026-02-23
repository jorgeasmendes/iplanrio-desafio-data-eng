{{ config(
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_id_terc ON {{ this }} (id_terc);
        CREATE INDEX IF NOT EXISTS idx_mes_carga ON {{ this }} (mes_carga)"
    ]
) }}

SELECT
    id_terc,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS sg_orgao_sup_tabela_ug,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS cd_ug_gestora,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_ug_tabela_ug,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS sg_ug_gestora,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nr_contrato,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nr_cnpj,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_razao_social,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nr_cpf,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_terceirizado,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_categoria_profissional,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_escolaridade,
    nr_jornada,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_unidade_prestacao,
    vl_mensal_salario,
    vl_mensal_custo,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS sg_orgao,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS nm_orgao,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS cd_orgao_siafi,
    COALESCE(sg_orgao_sup_tabela_ug, 'Não informado') AS cd_orgao_siape,
    COALESCE(mes_carga_tabela, mes_referencia) AS mes_carga
    
FROM {{ ref('terceirizados_silver') }}
