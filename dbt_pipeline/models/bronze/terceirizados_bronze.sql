{{ config(
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_mes_referencia ON {{ this }} (mes_referencia)"
    ]
) }}

SELECT *
FROM {{ source('raw', 'terceirizados_raw') }}
