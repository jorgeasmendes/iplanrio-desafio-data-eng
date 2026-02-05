# Desafio de Data Engineer - IPLANRIO
Repositório de instrução para o desafio técnico para vaga de Pessoa Engenheira de Dados.

## Descrição do desafio

Neste desafio você deverá capturar, estruturar, armazenar e transformar dados de Terceirizados de Órgãos Federais, disponíveis no site [Dados Abertos - Terceirizados de Órgãos Federais](https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados).

Para o desafio, será necessário construir uma pipeline que realiza a extração, processamento e transformação dos dados. Salve os dados de cada mes em um arquivo CSV (estruture os dados da maneira que achar mais conveniente, você tem liberdade para criar novas colunas ou particionar os dados), então carregue os dados para uma tabela no Postgres. Por fim, crie uma tabela derivada usando o DBT. A tabela derivada deverá seguir a padronização especificada no [manual de estilo da IPLANRIO](https://docs.dados.rio/data-lake/guia-de-estilo/convencoes-colunas). A solução devera contemplar o surgimento de novos dados a cada 4 meses.


## O que iremos avaliar

- **Completude**: A solução proposta atende a todos os requisitos do desafio?
- **Simplicidade**: A solução proposta é simples e direta? É fácil de entender e trabalhar?
- **Organização**: A solução proposta é organizada e bem documentada? É fácil de navegar e encontrar o que se procura?
- **Criatividade**: A solução proposta é criativa? Apresenta uma abordagem inovadora para o problema proposto?
- **Boas práticas**: A solução proposta segue boas práticas de Python, Git, Docker, etc.?

## Etapas

1. Subir o ambiente local com docker compose.
2. Construir pipeline de ingestão.
3. Persistir os dados mensais em CSVs particionados.
4. Carregar os dados no Postgres (tabela raw/staging).
5. Criar tabela derivada via dbt, aplicando a padronização de colunas conforme o guia da IPLANRIO.
6. Prever o surgimento de novos dados a cada ~4 meses (idempotência, reprocessamento incremental, detecção de novidades).

## Extras

- Commits seguindo o padrão Conventional Commits
- Arquivos .yml contendo descrições detalhadas de cada modelo e campo.
- Testes de qualidade de dados no DBT
- Estrutura de pastas e código organizada e legível
- Instruções claras de execução no README.md

## 🚨 Atenção

- A solução desse desafio deve ser publicada em um fork deste repositório no GitHub.
- O link do repositório deve ser enviado, para o e-mail utilizado para contato com o assunto "Desafio Data Engineer - IPLANRIO".
- Você deve ser capaz de apresentar sua solução, explicando como a idealizou, caso seja aprovado(a) para a próxima etapa.

## Links de referência / utilidades

- Documentação [Prefect](https://docs.prefect.io/v3/get-started)
- Documentação [DBT](https://docs.getdbt.com/docs/introduction)
- [Dados Abertos - Terceirizados de Órgãos Federais](https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados)
- Repositório pipelines da [IPLANRIO](https://github.com/prefeitura-rio/pipelines)
- Repositório de modelos DBT da [IPLANRIO](https://github.com/prefeitura-rio/queries-rj-iplanrio)
- [Manual de estilo da IPLANRIO](https://docs.dados.rio/data-lake/guia-de-estilo/convencoes-datasets-e-tabelas)
  
## Dúvidas?

Fale conosco pelo e-mail que foi utilizado para o envio desse desafio.
