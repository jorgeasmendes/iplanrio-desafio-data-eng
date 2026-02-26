SELECT 
    id_terceirizado
FROM {{ ref('terceirizados_silver') }}
WHERE mes_carga_tabela != mes_referencia