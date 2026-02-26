{{ config(
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_id_terceirizado ON {{ this }} (id_terceirizado);
        CREATE INDEX IF NOT EXISTS idx_mes_carga ON {{ this }} (mes_carga)"
    ]
) }}

SELECT
    id_terceirizado,
    COALESCE(terceirizado_cpf, 'Não informado') AS terceirizado_cpf,
    COALESCE(terceirizado_nome, 'Não informado') AS terceirizado_nome,
    COALESCE(terceirizado_categoria_profissional, 'Não informado') AS terceirizado_categoria_profissional,
    COALESCE(terceirizado_escolaridade, 'Não informado') AS terceirizado_escolaridade,
    terceirizado_salario,
    terceirizado_custo,
    jornada_horas,
    COALESCE(empresa_cnpj, 'Não informado') AS empresa_cnpj,
    COALESCE(empresa_razao_social, 'Não informado') AS empresa_razao_social,
    COALESCE(contrato_numero, 'Não informado') AS contrato_numero,
    COALESCE(orgao_superior_sigla, 'Não informado') AS orgao_superior_sigla,
    COALESCE(unidade_gestora_sigla, 'Não informado') AS unidade_gestora_sigla,
    COALESCE(unidade_gestora_nome, 'Não informado') AS unidade_gestora_nome,
    COALESCE(unidade_gestora_codigo, 'Não informado') AS unidade_gestora_codigo,
    COALESCE(orgao_sigla, 'Não informado') AS orgao_sigla,
    COALESCE(orgao_nome, 'Não informado') AS orgao_nome,
    COALESCE(orgao_codigo_siafi, 'Não informado') AS orgao_codigo_siafi,
    COALESCE(orgao_codigo_siape, 'Não informado') AS orgao_codigo_siape,
    COALESCE(unidade_prestacao_nome, 'Não informado') AS unidade_prestacao_nome,
    COALESCE(mes_carga_tabela, mes_referencia) AS mes_carga
    
FROM {{ ref('terceirizados_silver') }}
