# Business Intelligence Portfolio

> **Hypothetical Showcase:** Demonstrates BI and data visualisation skills across Tableau and Looker. The LookML examples are based on real dashboard patterns I've built for engineering velocity and DORA metrics reporting. All company names, org identifiers, and credentials are fully anonymised. No proprietary data is included.

---

## Tableau Public

Interactive dashboards built in Tableau — publicly available, no login required.

**[View my Tableau Public profile →](https://public.tableau.com/app/profile/derek.ohalloran)**

---

## LookML Examples ([`lookml/`](./lookml))

LookML is Looker's modelling language — a YAML-like syntax that defines how Looker generates SQL, structures explores (the self-serve query interface), and builds reusable dimension/measure libraries. The examples here cover the **Engineering Velocity** subject area, powered by the GitHub Pipelines data model.

### Why LookML matters

Raw SQL dashboards break when your schema changes and only one person can maintain them. LookML solves this by centralising business logic in a version-controlled layer:

- Dimensions and measures are defined once and reused across every dashboard
- Business rules (e.g. "exclude bots", "only SHA-matched deployments") are enforced at the model level — analysts can't accidentally violate them
- Explores control what joins are available, preventing accidental fan-out on many-to-many relationships
- `sql_always_where` filters apply globally to an explore, so defaults like "current employees only" don't need to be added to every tile

---

### Project Structure

```
lookml/
├── engineering_velocity.model.lkml     Model file — defines database connection
│                                       and the two explores (pr_velocity, dora_lead_time)
│
├── views/
│   ├── pr_facts.view.lkml              PR velocity metrics
│   │     Dimensions: org, repo, author LDAP, author type, state, branch, timestamps
│   │     Measures: PR count, merged count, merge rate,
│   │               avg/median/P75 time to merge
│   │
│   ├── lead_time_to_deploy.view.lkml   DORA lead time metrics
│   │     Dimensions: service, org, DORA bucket, match scenario, staging flags, timestamps
│   │     Measures: count, median lead time, P95 lead time,
│   │               avg merge→prod, avg time on staging, SHA match rate, elite %
│   │
│   └── dim_users.view.lkml             SCD Type 2 engineer dimension
│         Dimensions: LDAP, GitHub login, team, function, org hierarchy,
│                     employment type, SCD validity dates, is_current flag
│         Measures: engineer count (distinct LDAP)
│
└── dashboards/
    └── dora_metrics.dashboard.lkml     DORA Metrics dashboard definition
          Tiles: KPI row (median / P95 / elite % / SHA match rate)
                 Weekly lead time trend with DORA benchmark lines
                 DORA bucket distribution (donut)
                 Lead time by service (top 20 bar chart)
                 Lead time by team (bar chart)
                 Staging vs production time breakdown (stacked bar)
```

---

### [`engineering_velocity.model.lkml`](./lookml/engineering_velocity.model.lkml)

The model file is the entry point for Looker. It declares the database connection and defines two explores:

**`pr_velocity` explore** — answers questions about PR cycle time and review coverage:
- How long does it take PRs to get reviewed and merged, by team or repo?
- What % of PRs are merged without any external review (self-merges)?
- How has cycle time changed week-over-week?

```lookml
explore: pr_velocity {
  sql_always_where: ${pr_facts.author_type} = 'human' ;;  -- bots excluded by default
  join: dim_users { ... }                                  -- team attribution
  join: pr_review_summary { ... }                          -- review stats
}
```

**`dora_lead_time` explore** — answers DORA lead time questions:
- What's the median/P95 lead time by service or team?
- What % of deployments are in the "elite" DORA tier (<1 hour)?
- How much time do services spend in staging vs production promotion?

```lookml
explore: dora_lead_time {
  sql_always_where: ${lead_time_to_deploy.prod_match_scenario} = 'sha_match' ;;  -- high-confidence only
  join: pr_facts { ... }
  join: dim_users { ... }
}
```

---

### [`views/pr_facts.view.lkml`](./lookml/views/pr_facts.view.lkml)

Exposes `FACT_PULL_REQUESTS` to Looker. Key design decisions:

- **`drill_fields`** on the count measure — clicking a bar chart drops into a list of individual PRs with a direct GitHub link
- **`link`** on `pr_number` — generates a clickable "Open in GitHub" URL using the stored URL field
- **Three cycle time measures** (avg, median, P75) — median is most useful for skewed distributions; P75 surfaces the long tail
- `author_type` dimension uses a `case` block to show friendly labels ("Human", "Bot") in Looker UI while keeping raw values for filtering

---

### [`views/lead_time_to_deploy.view.lkml`](./lookml/views/lead_time_to_deploy.view.lkml)

Exposes `LEAD_TIME_TO_DEPLOY` to Looker. Key design decisions:

- **`is_sha_match` yesno dimension** — used as a default filter in measures (`filters: [is_sha_match: "Yes"]`) so every metric is automatically high-confidence unless the analyst explicitly overrides it
- **`dora_bucket_sort` hidden dimension** — LookML doesn't have a native "sort by another field" for string dimensions; this hidden numeric field controls ordering so buckets always display elite → high → medium → low, not alphabetically
- **`pct_sha_matched` measure** — a data quality KPI surfaced directly in the BI layer; if this drops below 80%, the deployment tooling needs attention
- **Separate measures for staging vs production timing** — `avg_time_on_staging_hours` is only meaningful when `has_staging_deployment = Yes`, so the filter is baked into the measure definition

---

### [`views/dim_users.view.lkml`](./lookml/views/dim_users.view.lkml)

SCD Type 2 engineer dimension. Key design decisions:

- **`is_current` filter pattern** — the model-level explore joins with `AND ${dim_users.is_current}` for all current-state dashboards. Point-in-time reports (e.g. "what team was this engineer on when the PR merged?") override this with a date-range join instead
- **`engineer_count` uses `count_distinct`** — prevents inflation when the dimension is joined to a many-to-one fact table
- Org hierarchy exposed at three levels (`team_name`, `function_l1_name`, `org_name`) so dashboards can be sliced at any level without schema changes

---

### [`dashboards/dora_metrics.dashboard.lkml`](./lookml/dashboards/dora_metrics.dashboard.lkml)

A complete DORA Metrics dashboard defined in LookML. Benefits of dashboard-as-code:
- Version controlled — dashboard changes go through code review
- Reproducible — deploy the same dashboard to dev/staging/prod environments
- Parameterised — date range, org, and service filters are pre-wired and reusable

The dashboard has six sections:
1. **KPI row** — four single-value tiles: median lead time, P95, elite %, SHA match rate
2. **Weekly trend** — line chart with reference lines at the DORA "high" (24h) and "elite" (1h) thresholds
3. **Bucket distribution** — donut chart showing the proportion of deployments in each DORA tier
4. **By service** — horizontal bar chart (top 20 services by median lead time)
5. **By team** — same view sliced by engineering team using the SCD2 user dimension
6. **Staging breakdown** — stacked bar showing how much time is spent in staging vs the merge→prod journey

---

## Tech Stack

`Looker` · `LookML` · `Snowflake` · `Tableau`
