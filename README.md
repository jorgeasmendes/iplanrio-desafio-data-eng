# Desafio de Data Engineer - IPLANRIO

Desafio técnico do processo seletivo para vaga de Pessoa Engenheira de Dados na IPLANRIO.

## Descrição da solução
O desafio era capturar, estruturar, armazenar e transformar dados de Terceirizados de Órgãos Federais a partir do site https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados na AWS S3 e deixá-los prontos para consulta em uma API. Abaixo, cada uma das etapas realizadas: 

### Pipeline de dados
Construção de um pipeline de dados orquestrado com Prefect rodando em containers Docker (um container com servidor e outro com o worker), contendo 4 tasks:
1. **create_bucket:** Cria bucket na AWS S3 se ele não existir. Se já existir e o usuário tiver acesso, apenas segue para a próxima task. Se o usuário não tiver acesso, encerra o pipeline;

2. **load_raw_data:** Faz o download dos dados de servidores terceirizados do governo federal a partir do site indicado. Em seguida, sobe os dados no formato parquet para o bucket S3 particionando pelo mês de carga no diretório **'[nome do bucket]/terceirizados/raw/*/*.parquet'**. Se os dados do mês de carga já existirem, são sobrescritos, garantindo idempotência. Por padrão, há uma busca automática por dados novos (posteriores ao último mês de carga existente no banco de dados), mas é possível desabilitá-la para selecionar manualmente um filtro com a faixa de tempo desejada (ano de início, mês de início, ano de fim e mês de fim, de forma inclusiva). 

3. **dbt_run:** Roda job DBT que cria um banco de dados local DuckDB com três camadas:
   - **Bronze:** cópia simples dos arquivos Parquet brutos. Índice na coluna referente ao mês de carga dos dados.
   - **Silver:** dados convertidos para os tipos corretos, colunas renomeadas e reordenadas. Índice na coluna referente ao mês de carga dos dados.
   - **Gold:** substituição de valores nulos por 'Não informado' e exclusão de colunas redundantes. Índice na coluna referente ao mês de carga dos dados e no ID do terceirizado, para agilizar consultas. 
   
   O DBT é executado sempre sobre a tabela inteira (sem lógica incremental) porque:
   - Os dados são criados a cada 4 meses, e o volume não é muito grande.
   - O desafio pede arquivos `.duckdb` em cada camada, então o incremental exigiria merge posterior, gerando overhead desnecessário.

4. **load_transformed_data:** Copia cada uma das tabelas do banco local como um arquivo de banco de dados .duckdb independente no bucket S3 no respectivo diretório, sobrescrevendo se o arquivo já existir (garantia de idempotência).

O Flow possui parâmetros para personalizar sua execução de acordo com a necessidade:
- **tasks:** Selecionar quais tasks serão executadas. As opções são: 
    1. "Criar bucket" (create_bucket), 
    2. "Carregar dados brutos" (load_raw_data) e 
    3. "Rodar DBT" (dbt_run e load_transformed_data).
    - Default: todos.
- **busca_automatica_dados_novos:** Carregar automaticamente apenas dados posteriores à última data de carga (mes_referencia) já carregados.
- **comando_dbt:** Comando a ser executado pelo DBT, podendo ser "build", "run" ou "test". Se for "test", a task load_transformed_data não é executada, por ser desnecessária. 
    - Default: "build".   

Se `busca_automatica_dados_novos` for desabilitada, os dados buscados são filtrados pelos seguintes parâmetros:
- **ano_inicio_carga** Filtro de ano de início de carga dos dados. 
    - Default: "2019". 
- **mes_inicio_carga:** Filtro de mês de início de carga dos dados. 
    - Default: "01", 
- **ano_fim_carga:** Filtro de ano de fim de carga dos dados. 
    - Default: "2050".
- **mes_fim_carga:** Filtro de mês de fim de carga dos dados.   
    - Default: "01".

### API para consulta dos dados
Criação de uma API com FastAPI, rodando também em Docker, que consome os dados da camada gold criada no pipeline do Prefect e salva localmente para agilizar as consultas. Possui os seguintes entrypoints:
1. **'/terceirizados'**: consulta dos dados de todos os terceirizados retornando apenas algumas colunas. Possui paginação.
2. **'/terceirizados/{id}'**: consulta de um terceirizado específico a partir de seu id, retornando todas as colunas da tabela.

## Instruções para replicar localmente
### Pré requisitos
- Docker instalado no computador
- Conta ativa na AWS (para criar o bucket S3 de armazenamento dos dados)

### Instruções
1. Clone este diretório no seu computador

2. Preencha o arquivo `.env_preencher` e depois o renomeie para `.env`. As variáveis de ambiente necessárias são as seguintes:
- AWS_ACCESS_KEY_ID=[Chave de acesso da AWS]
- AWS_SECRET_ACCESS_KEY=[Chave secreta da AWS]
- AWS_REGION=[Região da AWS onde ficará o bucket]
- BUCKET_NAME=[Nome do bucket onde deseja salvar as tabelas. Pode ser já existente se você tiver acesso ou um nome inédito em toda AWS]

3. Com o terminal, vá para a pasta raiz do projeto e execute o comando:
> docker compose up --build -d

4. Aguarde até que todos os containers sejam criados e acesse os seguintes endereços:
- **https://localhost/4200**: Servidor Prefect com o Flow já carregado e pronto para execução.
- **https://localhost/8000**: API para consulta dos dados. Só vai funcionar depois de rodar o Flow do prefect ao menos uma vez.
- **https://localhost/4000**: Documentação DBT das tabelas criadas 