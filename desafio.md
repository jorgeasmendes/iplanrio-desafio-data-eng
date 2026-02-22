# Desafio de Data Engineer - IPLANRIO

Repositório de instrução para o desafio técnico para vaga de Pessoa Engenheira de Dados.

## Descrição do desafio

Neste desafio você deverá capturar, estruturar, armazenar e transformar dados de Terceirizados de Órgãos Federais, disponíveis no site [Dados Abertos - Terceirizados de Órgãos Federais](https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados).

Para o desafio, será necessário construir uma pipeline que realiza a extração, processamento e transformação dos dados. Você deverá utilizar a arquitetura de medalhão para organizar os dados [^1]. Salve os dados de cada mês em arquivos Parquet (estruture os dados da maneira que achar mais conveniente, você tem liberdade para criar novas colunas ou particionar os dados), então carregue os dados em um bucket S3. Carregue os dados do bucket para um banco local DuckDB [^2], essa será sua camada bronze. Usando DBT e DuckDB [^3], crie bancos locais com as camadas prata e ouro e suba os arquivos no mesmo bucket S3. A tabela derivada deverá seguir a padronização especificada no [manual de estilo da IPLANRIO](https://docs.dados.rio/data-lake/guia-de-estilo/convencoes-colunas). A solução devera contemplar o surgimento de novos dados a cada 4 meses, ou seja, deve ser idempotente e capaz de detectar e processar novos dados de maneira incremental.

Ao final do processo de ELT, a organização do bucket S3 deve seguir algo semelhante a isso:

```
terceirizados/
├── raw/
│   ├── terceirizados_2025-01.parquet
│   ├── terceirizados_2025-05.parquet
│   ├── terceirizados_2025-07.parquet
│   └── ...
├── bronze/
│   └── terceirizados-bronze.duckdb
├── silver/
│   └── terceirizados-silver.duckdb
└── gold/
    └── terceirizados-gold.duckdb
```

Após isso, crie uma API REST utilizando os dados da camada ouro, expondo os dados de maneira organizada e fácil de consumir. A API deve ter dois endpoints:

- `/terceirizados`: retorna a lista completa de terceirizados. Implemente paginação para evitar sobrecarga de dados.
- `/terceirizados/{id}`: retorna os detalhes de um terceirizado específico, identificado pelo seu ID.

Dados que precisam estar presentes no endpoint `/terceirizados`:

- ID do terceirizado
- Sigla do órgão superior da unidade gestora do terceirizado
- CNPJ da empresa terceirizada
- CPF do terceirizado

No endpoitnt `/terceirizados/{id}`, todos os dados presentes na camada ouro devem ser retornados [^4]. Implemente também um mecanismo simples de cache para otimizar o desempenho da API, reduzindo o tempo de resposta para requisições frequentes. O acesso aos dados da camada ouro pode ser feito do modo que preferir, seja lendo diretamente do arquivo DuckDB direto do bucket ou carregando os dados para um banco de dados relacional (PostgreSQL, MySQL, etc.) e consultando a partir dele. A implementação de testes para a API é opcional, mas será considerada um diferencial. Adição de ferramentas de observabilidade e integração com [OpenTelemetry](https://opentelemetry.io/docs/) também é um diferencial.

## O que iremos avaliar

- **Completude**: a solução proposta atende a todos os requisitos do desafio?
- **Simplicidade**: a solução proposta é simples e direta? É fácil de entender e trabalhar?
- **Organização**: a solução proposta é organizada e bem documentada? É fácil de navegar e encontrar o que se procura?
- **Criatividade**: a solução proposta é criativa? Apresenta uma abordagem inovadora para o problema proposto?
- **Boas práticas**: a solução proposta segue boas práticas de Python, Git, Docker, etc.?

## Tecnologias obrigatórias

- Docker e Docker Compose: para orquestração de containers
- Prefect (v3): para orquestração de pipelines
- DuckDB: para armazenamento e consulta dos dados
- DBT: para transformação dos dados
- Algum REST framework: você pode escolher a linguagem e o framework de sua preferência (FastAPI, Flask, Express, Spring Boot, etc.)
- Git e GitHub: para controle de versão e hospedagem do código
- S3 (AWS S3, GCS, Backblaze B2, etc): para armazenamento dos arquivos Parquet [^5]

## Etapas

1. Subir o ambiente local com Docker Compose
2. Construir pipeline de ingestão
3. Persistir os dados mensais em arquivos Parquet particionados por mês
4. Fazer upload dos arquivos Parquet para um bucket S3 [^6]
5. Carregar os dados do bucket S3 para o DuckDB (camada bronze)
6. Criar camadas de dados via DBT, aplicando a padronização de colunas conforme o guia da IPLANRIO
7. Fazer upload dos arquivos DuckDB para o bucket S3, organizando as camadas de dados (bronze, prata e ouro)
8. Criar uma API REST para expor os dados da camada ouro, implementando os endpoints `/terceirizados` e `/terceirizados/{id}` e um mecanismo de cache para otimizar o desempenho da API
9. Prever o surgimento de novos dados a cada ~4 meses (idempotência, reprocessamento incremental, detecção de novidades)

## Instruções extras

- Faça commits seguindo o padrão Conventional Commits
- Adicione metadados contendo descrições detalhadas de cada modelo e campo usando as ferramentas disponíveis no DBT
- Adicione testes de qualidade de dados no DBT
- Use uma estrutura de pastas e código organizada e legível
- Adicione instruções claras de execução no README.md

## 🚨 Atenção

- A solução desse desafio deve ser publicada em um fork deste repositório no GitHub.
- O link do repositório deve ser enviado para o e-mail utilizado para contato com o assunto "Desafio Data Engineer - IPLANRIO".
- Você deve ser capaz de apresentar sua solução, explicando como a idealizou, caso seja aprovado(a) para a próxima etapa.
- Caso não consigamos replicar a solução proposta, ou caso haja problemas de acesso ao código, dados ou infraestrutura utilizada, a solução não será avaliada e o candidato não passará para a próxima etapa do processo seletivo.

## Links de referência

- [Prefect](https://docs.prefect.io/v3/get-started)
- [DBT](https://docs.getdbt.com/docs/introduction)
- [Dados Abertos - Terceirizados de Órgãos Federais](https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados)
- [Repositório pipelines da IPLANRIO](https://github.com/prefeitura-rio/prefect_rj_iplanrio)
- [Repositório de modelos DBT da IPLANRIO](https://github.com/prefeitura-rio/queries-rj-iplanrio/)
- [Manual de estilo da IPLANRIO](https://docs.dados.rio/data-lake/guia-de-estilo/convencoes-datasets-e-tabelas)

## Dúvidas?

Fale conosco pelo e-mail que foi utilizado para o envio desse desafio.

[^1]: Guia: <https://www.databricks.com/glossary/medallion-architecture>

[^2]: Guia: <https://duckdb.org/docs/stable/guides/network_cloud_storage/s3_import>

[^3]: Guia: <https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup>

[^4]: Sinta-se à vontade para adicionar filtros, paginação ou outros mecanismos para otimizar o desempenho da API. Para mais informações, consulte a descrição e o dicionário de dados oficial: <https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados/arquivos/descricao-e-dicionario-de-dados.pdf>

[^5]: Você pode também utilizar soluções locais que emulem S3, como [MinIO](https://www.min.io/), [RustFS](https://rustfs.com/) ou [SeaweedFS](https://seaweedfs.com/).

[^6]: Envie as credenciais de acesso ao bucket S3 utilizado para o e-mail de contato, caso seja necessário para a avaliação da solução. Lembre-se de não expor chaves de acesso ou informações sensíveis no código, utilize variáveis de ambiente ou arquivos de configuração para isso. Após a avaliação, sinta-se à vontade para excluir os arquivos e credenciais do bucket S3 utilizado.
