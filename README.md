# CRM Churn Pipeline with DuckDB

Pipeline de engenharia de dados que transforma dados brutos de CRM em
uma base analítica confiável para **análise de churn**, com foco em
**qualidade de dados**, **modelagem consistente** e **validação
orientada a negócio**.

------------------------------------------------------------------------

## Storytelling (Contexto de consultoria)

Este projeto simula um cenário real de consultoria em engenharia de
dados.

Ao iniciar a análise, os dados apresentavam problemas clássicos:

-   clientes duplicados (mesmo email)
-   interações sem cliente válido
-   duração negativa em eventos
-   transações inconsistentes
-   registros com datas futuras
-   campanhas com janelas inválidas

Ou seja: **dados reais, imperfeitos e perigosos para análise**.

O desafio não era apenas processar os dados, mas:

> **tomar decisões corretas sobre dados incorretos**

A solução foi construída como um pipeline completo, separando claramente
cada responsabilidade e garantindo rastreabilidade de todas as decisões.

------------------------------------------------------------------------

## Objetivo do projeto

Construir um pipeline em Python + DuckDB capaz de:

-   ingerir dados brutos de CRM
-   diagnosticar problemas de qualidade
-   aplicar regras de limpeza e padronização
-   consolidar uma visão única de cliente (`customer_360`)
-   gerar features analíticas para churn
-   validar resultados com queries de negócio
-   exportar datasets finais

------------------------------------------------------------------------

## Arquitetura do pipeline

``` mermaid
flowchart TD
    A[pipeline.py<br/>Orquestra execução] --> B[src/config.py<br/>Validação config]
    A --> C[src/extract.py<br/>Leitura dos dados]
    A --> D[src/db.py<br/>DuckDB]

    C --> E[(Raw tables)]
    D --> E

    E --> F[src/quality.py<br/>Data Quality Report]
    E --> G[src/transform.py<br/>Cleaning]

    G --> H[(customer_360)]
    G --> I[(clean_interactions)]
    G --> J[(clean_transactions)]
    G --> K[(clean_campaigns)]

    H --> L[src/features.py<br/>Feature engineering]
    I --> L
    J --> L
    K --> L

    L --> M[(churn_features)]

    M --> N[src/sql_runner.py<br/>Validações Q1-Q4]
    M --> O[src/load.py<br/>Exportação]
```

------------------------------------------------------------------------

## Data Quality First (diferencial do projeto)

Antes de qualquer transformação, o pipeline executa um diagnóstico
completo de qualidade:

-   contagem de registros
-   nulos em colunas-chave
-   integridade referencial
-   duplicidade de clientes
-   anomalias de dados
-   inconsistências temporais
-   resumo executivo com impacto

Isso garante que **todas as decisões de cleaning são justificadas**,
e não arbitrárias.

------------------------------------------------------------------------

## Principais decisões técnicas

### 1. Deduplicação de clientes

-   baseada em email padronizado (`lower + trim`)
-   mantém o registro mais antigo (`created_at`)

### 2. Integridade referencial

-   interações órfãs são removidas

### 3. Duração negativa

-   convertida para `NULL`

### 4. Compras com valor zero

-   mantidas com flag (`amount_flag`)

### 5. Fonte não confiável

-   `LEGACY_V2` removida

### 6. Campanhas inválidas

-   removidas se `start_date > end_date`

------------------------------------------------------------------------

## Modelagem analítica

Camadas:

-   `customer_360`
-   `clean_interactions`
-   `clean_transactions`
-   `clean_campaigns`
-   `churn_features`

------------------------------------------------------------------------

## Tabela final: churn_features

-   recency_days\
-   interaction_count_90d\
-   purchase_count\
-   days_since_last_purchase\
-   total_revenue\
-   campaign_response_rate\
-   has_test_drive\
-   avg_days_between_services

## Fluxo de execução

1.  valida config\
2.  carrega dados\
3.  registra no DuckDB\
4.  roda quality report\
5.  transforma dados\
6.  gera features\
7.  valida SQL\
8.  exporta

------------------------------------------------------------------------

## Estrutura do projeto

    data-engineering-crm-pipeline/
        ├─ data/
        │  ├─ raw/
        ├─ output/
        ├─ sql/
        │  └─ queries.sql
        ├─ src/
        │  ├─ config.py
        │  ├─ db.py
        │  ├─ extract.py
        │  ├─ features.py
        │  ├─ load.py
        │  ├─ quality.py
        │  ├─ sql_runner.py
        │  ├─ transform.py
        │  └─ utils.py
        ├─ pipeline.py
        ├─ requirements.txt
        └─ README.md

------------------------------------------------------------------------

## Tecnologias

-   Python\
-   DuckDB\
-   Pandas

------------------------------------------------------------------------

## Como executar

    python -m venv .venv
    pip install -r requirements.txt
    python pipeline.py

------------------------------------------------------------------------

## Outputs

-   customer_360\
-   clean_interactions\
-   clean_transactions\
-   clean_campaigns\
-   churn_features
------------------------------------------------------------------------

## Evoluções

-   testes\
-   Airflow\
-   Docker\
-   cloud

------------------------------------------------------------------------

## Resumo

Pipeline completo de engenharia de dados focado em qualidade e valor
analítico.


------------------------------------------------------------------------

## Schema Design

O modelo adotado segue uma abordagem em camadas (inspirada no padrão medalhão):

- **Raw Layer** → dados brutos, sem alteração
- **Clean Layer** → dados tratados e padronizados
- **Feature Layer** → dados enriquecidos para análise de churn

Essa separação garante:
- rastreabilidade das transformações
- reprocessamento seguro
- isolamento de regras de negócio

------------------------------------------------------------------------

## Assumptions

Durante o desenvolvimento, algumas decisões foram tomadas:

- Clientes sem interações são considerados inativos
- Telefones inválidos são tratados como `NULL`
- Compras com valor zero representam edge cases válidos (não erro)
- Campanhas com janela inválida são removidas (não corrigidas)
- Apenas fontes confiáveis são utilizadas para análise temporal

------------------------------------------------------------------------

## One Thing That Surprised Me

A presença de **35 interações com datas futuras**, todas concentradas no `source_system = LEGACY_V2`, foi um achado inesperado.

Isso indicou fortemente um problema de ingestão ou ambiente de teste, e mostrou como uma única fonte pode distorcer completamente métricas críticas como **recência**, impactando diretamente modelos de churn.

------------------------------------------------------------------------

## Architecture Memo

### (A) Triggered Emails (SLA de 2 horas)

**Solução proposta:**
- Implementar arquitetura orientada a eventos (event-driven)
- Capturar eventos via CDC ou streaming
- Publicar em fila (Kafka / PubSub)
- Processar em tempo quase real

**Trade-off:**
- Maior complexidade operacional em troca de baixa latência

**O que NÃO fazer:**
- Não usar pipeline batch → incapaz de atender SLA de 2h

---

### (B) Clientes ausentes (~15%)

**Solução proposta:**
- Criar camada de resolução de identidade
- Ingerir clientes a partir de interações (fallback)
- Evoluir para modelo de Golden Record

**Trade-off:**
- Possível aumento de duplicidade vs maior cobertura de dados

**O que NÃO fazer:**
- Não ignorar esses clientes → gera viés no modelo de churn

------------------------------------------------------------------------

## What I Would Do With More Time

- Implementar testes automatizados (unit + data tests)
- Criar cargas incrementais
- Adicionar orquestração (Airflow)
- Implementar monitoramento e alertas
- Deploy em ambiente cloud

