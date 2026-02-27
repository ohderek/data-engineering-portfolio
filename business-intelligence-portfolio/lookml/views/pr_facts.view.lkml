# pr_facts.view.lkml
# ─────────────────────────────────────────────────────────────────────────────
# View over FACT_PULL_REQUESTS — the central PR grain table.
#
# Exposes PR velocity metrics (cycle time, review coverage, merge rate) and
# author/org dimensions for slicing. Designed to be joined via the
# engineering_velocity explore in engineering_velocity.model.lkml.
# ─────────────────────────────────────────────────────────────────────────────

view: pr_facts {
  sql_table_name: GITHUB_PIPELINES.REPORTING_TABLES.FACT_PULL_REQUESTS ;;

  # ── Primary key ──────────────────────────────────────────────────────────

  dimension: pr_pk {
    primary_key: yes
    hidden:      yes
    type:        string
    sql:         ${TABLE}.PK ;;
  }


  # ── Identifiers ──────────────────────────────────────────────────────────

  dimension: pr_id {
    type:        number
    sql:         ${TABLE}.PR_ID ;;
    label:       "PR ID"
    description: "GitHub internal PR ID."
  }

  dimension: pr_number {
    type:        number
    sql:         ${TABLE}.NUMBER ;;
    label:       "PR Number"
    description: "PR number within the repository (visible in GitHub URL)."
    link: {
      label: "Open in GitHub"
      url:   "{{ pr_facts.url._value }}"
    }
  }

  dimension: url {
    type:        string
    sql:         ${TABLE}.URL ;;
    hidden:      yes
  }


  # ── Dimensions — org / repo context ──────────────────────────────────────

  dimension: org {
    type:        string
    sql:         ${TABLE}.ORG ;;
    label:       "Organisation"
    description: "GitHub organisation the PR belongs to."
  }

  dimension: repo_full_name {
    type:        string
    sql:         ${TABLE}.REPO_FULL_NAME ;;
    label:       "Repository"
    description: "Full repository name (org/repo)."
  }


  # ── Dimensions — author ───────────────────────────────────────────────────

  dimension: author_ldap {
    type:        string
    sql:         ${TABLE}.USER_LDAP ;;
    label:       "Author LDAP"
    description: "Internal LDAP identifier for the PR author."
  }

  dimension: author_login {
    type:        string
    sql:         ${TABLE}.USER_LOGIN ;;
    label:       "Author GitHub Login"
  }

  dimension: author_type {
    type:        string
    sql:         ${TABLE}.AUTHOR_TYPE ;;
    label:       "Author Type"
    description: "human | bot | service_account"

    case: {
      when: { sql: ${author_type} = 'human'           ;; label: "Human"           }
      when: { sql: ${author_type} = 'bot'             ;; label: "Bot"             }
      when: { sql: ${author_type} = 'service_account' ;; label: "Service Account" }
      else: "Unknown"
    }
  }


  # ── Dimensions — PR state ─────────────────────────────────────────────────

  dimension: state {
    type:        string
    sql:         ${TABLE}.STATE ;;
    label:       "State"
    description: "open | closed"
  }

  dimension: is_merged {
    type:        yesno
    sql:         ${TABLE}.MERGED ;;
    label:       "Is Merged"
  }

  dimension: is_draft {
    type:        yesno
    sql:         ${TABLE}.DRAFT ;;
    label:       "Is Draft"
  }

  dimension: base_branch {
    type:        string
    sql:         ${TABLE}.BASE_REF ;;
    label:       "Target Branch"
    description: "Branch the PR is merging into (e.g. main, master)."
  }


  # ── Dimensions — timestamps ───────────────────────────────────────────────

  dimension_group: created {
    type:        time
    timeframes:  [raw, date, week, month, quarter, year]
    sql:         ${TABLE}.CREATED_AT ;;
    label:       "Created"
    description: "When the PR was opened."
  }

  dimension_group: merged {
    type:        time
    timeframes:  [raw, date, week, month, quarter, year]
    sql:         ${TABLE}.MERGED_AT ;;
    label:       "Merged"
    description: "When the PR was merged. NULL if not merged."
  }

  dimension_group: closed {
    type:        time
    timeframes:  [raw, date, week, month, quarter, year]
    sql:         ${TABLE}.CLOSED_AT ;;
    label:       "Closed"
  }


  # ── Measures — counts ─────────────────────────────────────────────────────

  measure: count {
    type:        count
    label:       "PR Count"
    description: "Total number of pull requests."
    drill_fields: [pr_number, org, repo_full_name, author_ldap, state, created_date]
  }

  measure: merged_count {
    type:        count
    label:       "Merged PR Count"
    filters:     [is_merged: "Yes"]
    description: "Number of merged pull requests."
  }

  measure: open_count {
    type:        count
    label:       "Open PR Count"
    filters:     [state: "open"]
    description: "Number of currently open pull requests."
  }

  measure: merge_rate {
    type:        number
    label:       "Merge Rate"
    description: "Percentage of PRs that were merged (vs closed without merging)."
    sql:         ${merged_count} * 1.0 / NULLIF(${count}, 0) ;;
    value_format: "0.0%"
  }


  # ── Measures — cycle time ─────────────────────────────────────────────────

  measure: avg_time_to_merge_hours {
    type:        average
    label:       "Avg Time to Merge (hours)"
    description: "Average hours from PR creation to merge. Merged PRs only."
    sql: DATEDIFF('hour', ${TABLE}.CREATED_AT, ${TABLE}.MERGED_AT) ;;
    filters:     [is_merged: "Yes"]
    value_format: "0.0"
  }

  measure: median_time_to_merge_hours {
    type:        median
    label:       "Median Time to Merge (hours)"
    sql: DATEDIFF('hour', ${TABLE}.CREATED_AT, ${TABLE}.MERGED_AT) ;;
    filters:     [is_merged: "Yes"]
    value_format: "0.0"
  }

  measure: p75_time_to_merge_hours {
    type:        percentile
    percentile:  75
    label:       "P75 Time to Merge (hours)"
    sql: DATEDIFF('hour', ${TABLE}.CREATED_AT, ${TABLE}.MERGED_AT) ;;
    filters:     [is_merged: "Yes"]
    value_format: "0.0"
  }
}
