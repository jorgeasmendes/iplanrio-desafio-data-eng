# Desafio de Data Engineer - IPLANRIO

Desafio técnico do processo seletivo para vaga de Pessoa Engenheira de Dados na IPLANRIO.

## Descrição da solução
### Pipeline de dados
Construção de um pipeline de dados orquestrado com Prefect rodando em containers Docker (um container com servidor e outro com o worker), contendo 4 tasks:
1. **create_bucket:** Cria bucket na AWS S3 se ele não existir. Se já existir e o usuário tiver acesso, apenas segue para a próxima task. Se o usuário não tiver acesso, encerra o pipeline;

2. **load_raw_data:** Faz o download dos dados de servidores terceirizados do governo federal a partir do site https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados. Em seguida, sobe os dados no formato parquet para o bucket S3 particionando pelo mês de carga no diretório **'[nome do bucket]/terceirizados/raw/*/*.parquet'**. É possível filtrar pelo mês de carga dos dados selecionando a faixa de tempo desejada (ano de início, mês de início, ano de fim e mês de fim, de forma inclusiva);

3. **dbt_run:** Roda job DBT que cria um banco de dados local DuckDB com três camadas:
   - **Bronze:** cópia simples dos arquivos Parquet brutos com índice na coluna de partição.
   - **Silver:** dados convertidos para os tipos corretos.
   - **Gold:** substituição de valores nulos por 'Não informado', exclusão de colunas redundantes e criação de índice no ID do terceirizado para agilizar consultas. 
   
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
- **ano_inicio_carga** Filtro de ano de início de carga dos dados. 
    - Default: "2019". 
- **mes_inicio_carga:** Filtro de mês de início de carga dos dados. 
    - Default: "01", 
- **ano_fim_carga:** Filtro de ano de fim de carga dos dados. 
    - Default: "2050".
- **mes_fim_carga:** Filtro de mês de fim de carga dos dados.   
    - Default: "01".
- **comando_dbt:** Comando a ser executado pelo DBT, podendo ser "build", "run" ou "test". Se for "test", a task load_transformed_data não é executada, por ser desnecessária. 
    - Default: "build".   
### API para consulta dos dados
Criação de uma API com FastAPI, rodando também em Docker, com entrypoints:
1. **'/terceirizados'**: consulta dos dados de todos os terceirizados retornando apenas algumas colunas. Possui paginação.
2. **'/terceirizados/{id}'**: consulta de um servidor específico a partir de seu id, retornando todas as colunas da tabela.

## Instruções para replicar localmente
### Manualmente
1. Crie um arquivo .env com as seguintes variáveis de ambiente:
- AWS_ACCESS_KEY_ID=[Chave de acesso da AWS]
- AWS_SECRET_ACCESS_KEY=[Chave secreta da AWS]
- AWS_REGION=[Região da AWS onde ficará o bucket]
- BUCKET_NAME=[Nome do bucket onde deseja salvar as tabelas. Pode ser já existente se você tiver acesso ou um nome inédito em toda AWS]

2. Com o terminal, vá para a pasta raiz do projeto e execute o comando:
> docker compose up --build -d

3. Aguarde até que todos os containers sejam criados e acesse os seguintes endereços:
- **localhost/4200**: Servidor Prefect com o Flow já carregado e pronto para execução.
- **localhost/8000**: API para consulta dos dados. Só vai funcionar depois de rodar o Flow do prefect ao menos uma vez.