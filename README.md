<div align="center">

[![Typing SVG](https://readme-typing-svg.demolab.com?font=Fira+Code&weight=700&size=26&pause=1000&color=00D9FF&center=true&vWidth=900&lines=Hey+there%2C+I'm+Derek+%F0%9F%91%8B;Senior+Data+Engineer;Pipeline+Architect+%7C+dbt+Advocate;Snowflake+%E2%9D%84%EF%B8%8F+%7C+Airflow+%F0%9F%8C%AC+%7C+Prefect+%E2%9A%A1)](https://git.io/typing-svg)

<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=180&section=header&text=Data%20Engineer&fontSize=42&fontColor=fff&animation=twinkling&fontAlignY=32&desc=Turning%20Raw%20Data%20into%20Business%20Value&descAlignY=55&descSize=18" width="100%"/>

</div>

---

## About Me

```python
derek = {
    "role":       "Senior Data Engineer",
    "focus":      ["Pipeline Architecture", "Data Modeling", "Analytics Engineering"],
    "stack": {
        "orchestration":  ["Apache Airflow", "Prefect"],
        "transformation": ["dbt Core", "PySpark", "Pandas", "PyArrow"],
        "warehousing":    ["Snowflake", "BigQuery"],
        "languages":      ["Python", "SQL", "Bash"],
        "infra":          ["Terraform", "Docker", "GitHub Actions"],
        "quality":        ["Great Expectations", "dbt tests", "dbt_project_evaluator"],
    },
    "data_model_design": [
        "Dimensional modelling (fact + dimensions + bridges)",
        "SCD Type 2 for slowly-changing attributes",
        "DORA metrics: Lead Time, Deployment Frequency, Change Failure Rate",
        "Multi-source union with Jinja-driven dbt models",
    ],
    "currently_building": "GitHub Pipelines — DORA metrics platform",
}
```

---

## Tech Stack

<div align="center">

**Languages & Query**

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=for-the-badge&logo=gnubash&logoColor=white)

**Data Platforms & Warehousing**

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)

**Orchestration & Streaming**

![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-024DFD?style=for-the-badge&logo=prefect&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)

**Infrastructure & DevOps**

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)

</div>

---

## Featured Projects

### Tech Health Platform

Operational engineering health metrics — incidents, SLOs, AI developer experience (AI DX). Full data platform from API ingestion to Snowflake marts.

| Repo | Stack | Description |
|------|-------|-------------|
| [**tech-health/dbt-tech-health**](https://github.com/ohderek/data-engineering-portfolio) | `dbt` `Snowflake` | Staging → intermediate → marts with Jinja-looped multi-source unions |
| [**tech-health/etl-pipelines**](https://github.com/ohderek/data-engineering-portfolio) | `Airflow` `Prefect` `Python` | Modular ETL framework + 4 production DAGs/flows |

**Patterns showcased:**
- Jinja loops over incident workspaces and AI tools (eliminates N×copy-paste)
- Prefect flows with GCP Secret Manager blocks, PyArrow/GCS staging, Snowflake COPY+MERGE
- `BaseExtractor` / `DataTransformer` / `SnowflakeLoader` class hierarchy for Airflow

---

### CoinMarketCap → Snowflake

REST API scraping pipeline: fetches live crypto market data (prices, volumes, market cap, BTC dominance) and loads it into Snowflake for analytics.

| Repo | Stack | Description |
|------|-------|-------------|
| [**crypto-market-data**](https://github.com/ohderek/data-engineering-portfolio) | `Python` `httpx` `PyArrow` `Snowflake` | CoinMarketCap API → Parquet → Snowflake stage-and-merge + analytics views |

**Patterns showcased:**
- Paginated REST API client with rate-limit detection and exponential back-off
- PyArrow schema enforcement before Snowflake COPY INTO
- Stage-and-merge idempotent load (re-runnable without duplicates)
- `CLUSTER BY DATE(FETCHED_AT)` + `QUALIFY ROW_NUMBER()` analytics views

---

### GitHub Pipelines

DORA metrics and engineering velocity platform built on GitHub API data. ERD designed from scratch — implemented twice: once as raw SQL ETL jobs, once as a full dbt project. Covers Lead Time to Deploy, PR review coverage, commit file churn, and team benchmarking.

| Repo | Stack | Description |
|------|-------|-------------|
| [**github-analytics/sql-jobs**](https://github.com/ohderek/data-engineering-portfolio) | `Snowflake SQL` | Raw ETL scripts: multi-org consolidation, BRIDGE_GITHUB_LDAP, FACT_PULL_REQUESTS, 7-stage lead time pipeline |
| [**github-analytics/dbt-github-analytics**](https://github.com/ohderek/data-engineering-portfolio) | `dbt` `Snowflake` | Same model as dbt — staging → intermediate → marts, SCD2 user dim, incremental predicates |
| [**github-analytics/etl-pipeline**](https://github.com/ohderek/data-engineering-portfolio) | `Prefect` `PyArrow` | GitHub REST API → GCS → Snowflake RAW ingestion flow |

**Patterns showcased:**
- Raw SQL: `UNION BY NAME` multi-connector dedup with `QUALIFY ROW_NUMBER()`
- Raw SQL: 7-stage DORA lead time — SHA match → time-based fallback → final MERGE
- Raw SQL: Longest-prefix monorepo service attribution
- dbt: SCD Type 2 user dimension for point-in-time team attribution
- dbt: `incremental_predicates` for Snowflake micro-partition pruning
- Both: Noisy commit detection, DORA bucket classification (elite / high / medium / low)

---

### Business Intelligence

Looker + Tableau dashboards built on top of the data platforms above. LookML defines the semantic layer — dimensions, measures, explores, and dashboard-as-code — so business logic lives in version control, not individual tiles.

| Repo | Stack | Description |
|------|-------|-------------|
| [**business-intelligence-portfolio/lookml**](https://github.com/ohderek/data-engineering-portfolio) | `Looker` `LookML` | Engineering Velocity model: pr_velocity + dora_lead_time explores, SCD2 user join, DORA Metrics dashboard-as-code |
| [**Tableau Portfolio**](https://public.tableau.com/app/profile/derek.o.halloran/viz/Portfolio_54/Story1) | `Tableau` | Interactive public dashboards |

**Patterns showcased:**
- `sql_always_where` to enforce global filters (exclude bots, SHA-match only) at the explore level
- Hidden sort dimensions to control DORA bucket ordering in Looker UI
- Baking data quality KPIs (SHA match rate) directly into the BI layer as measures
- Dashboard-as-code: version-controlled tile layout, filters, and reference lines
- SCD Type 2 join pattern for point-in-time team attribution in self-serve reports

---

## Data Architecture Principles

```
Raw Sources  ──►  Bronze / RAW  ──►  Staging (stg_)  ──►  Intermediate (int_)  ──►  Marts (fct_ / dim_)
                   (Fivetran         (Thin ETL           (Business logic,          (Analyst-facing,
                    API loads,        wrappers,            multi-source              persist_docs,
                    Prefect flows)    type casts,          unions, Jinja loops,      cluster_by,
                                      PII hashing)         incremental merges)       dynamic WH sizing)
```

---

## GitHub Stats

<div align="center">
  <img height="170" src="https://github-readme-stats.vercel.app/api?username=ohderek&show_icons=true&theme=tokyonight&hide_border=true&count_private=true&include_all_commits=true" />
  <img height="170" src="https://github-readme-stats.vercel.app/api/top-langs/?username=ohderek&layout=compact&theme=tokyonight&hide_border=true&langs_count=8" />
</div>

<div align="center">
  <img src="https://github-readme-streak-stats.herokuapp.com/?user=ohderek&theme=tokyonight&hide_border=true" />
</div>

---

<div align="center">

**Let's connect**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/derek-o-halloran/)
[![Email](https://img.shields.io/badge/Email-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:ohalloran.derek@gmail.com)

<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=120&section=footer" width="100%"/>

</div>
