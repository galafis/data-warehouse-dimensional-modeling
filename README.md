# Data Warehouse Dimensional Modeling

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-013243?style=for-the-badge&logo=numpy&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)
![License-MIT](https://img.shields.io/badge/License--MIT-yellow?style=for-the-badge)


Toolkit de modelagem dimensional para Data Warehouses. Gerador de star/snowflake schema, tabelas de dimensao com SCD Types 1, 2 e 3, tabelas fato, gerenciamento de chaves substitutas e padroes ETL.

Dimensional modeling toolkit for Data Warehouses. Star/snowflake schema generator, dimension tables with SCD Types 1, 2 and 3, fact tables, surrogate key management, and ETL patterns.

---

## Arquitetura / Architecture

```mermaid
graph TB
    subgraph SchemaGenerator["Schema Generator"]
        SG[build_star_schema]
    end

    subgraph Dimensions["Tabelas Dimensao"]
        D1[DimensionTable SCD1]
        D2[DimensionTable SCD2]
        D3[DimensionTable SCD3]
    end

    subgraph Facts["Tabelas Fato"]
        F1[FactTable]
    end

    subgraph Support["Suporte"]
        SK[SurrogateKeyManager]
    end

    SG --> D1
    SG --> D2
    SG --> D3
    SG --> F1
    D1 --> SK
    D2 --> SK
    D3 --> SK
    D1 --> F1
    D2 --> F1
    D3 --> F1
```

## SCD Types / Tipos SCD

```mermaid
graph LR
    subgraph SCD1["SCD Tipo 1 - Sobrescrever"]
        S1A[Registro Original] -->|Update| S1B[Registro Atualizado]
    end

    subgraph SCD2["SCD Tipo 2 - Historico Completo"]
        S2A[v1 is_current=0] --> S2B[v2 is_current=0]
        S2B --> S2C[v3 is_current=1]
    end

    subgraph SCD3["SCD Tipo 3 - Valor Anterior"]
        S3A["city=RJ, previous_city=SP"]
    end
```

## Funcionalidades / Features

| Funcionalidade / Feature | Descricao / Description |
|---|---|
| SCD Type 1 | Sobrescreve valores existentes / Overwrites existing values |
| SCD Type 2 | Historico completo com valid_from/valid_to / Full history with valid_from/valid_to |
| SCD Type 3 | Armazena valor anterior em colunas separadas / Stores previous value in separate columns |
| Surrogate Key Manager | Geracao e mapeamento de chaves substitutas / Surrogate key generation and mapping |
| Fact Table | Insercao unitaria e em massa, agregacao / Single and bulk insert, aggregation |
| Schema Generator | Construcao declarativa de star schemas / Declarative star schema construction |

## Inicio Rapido / Quick Start

```python
from src.dimensional_modeling import SchemaGenerator

gen = SchemaGenerator()
schema = gen.build_star_schema({
    "dimensions": [
        {"name": "dim_product", "columns": {"product_id": "TEXT", "name": "TEXT"},
         "natural_key": "product_id", "scd_type": 1},
        {"name": "dim_customer", "columns": {"customer_id": "TEXT", "city": "TEXT"},
         "natural_key": "customer_id", "scd_type": 2},
    ],
    "facts": [
        {"name": "fact_sales", "measures": {"revenue": "REAL", "qty": "INTEGER"},
         "dimension_keys": {"product_sk": "dim_product", "customer_sk": "dim_customer"}},
    ],
})

# Load dimensions
gen.dimensions["dim_product"].load({"product_id": "P1", "name": "Widget"})
gen.dimensions["dim_customer"].load({"customer_id": "C1", "city": "SP"}, "2024-01-01")

# Load facts
gen.facts["fact_sales"].insert({"product_sk": 1, "customer_sk": 1, "revenue": 150.0, "qty": 3})
```

## Testes / Tests

```bash
pytest tests/ -v
```

## Tecnologias / Technologies

- Python 3.9+
- SQLite3
- pytest

## Licenca / License

MIT License - veja [LICENSE](LICENSE) / see [LICENSE](LICENSE).
