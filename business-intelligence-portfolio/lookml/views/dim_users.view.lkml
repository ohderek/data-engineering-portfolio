# dim_users.view.lkml
# ─────────────────────────────────────────────────────────────────────────────
# View over DIM_GITHUB__USERS — SCD Type 2 engineer dimension.
#
# Always filter on is_current = TRUE for current state.
# For point-in-time attribution (e.g. "what team was this engineer on when
# the PR was merged?") join using the date-range condition:
#   ON pr.merged_date BETWEEN dim.effective_start_date AND dim.effective_end_date
# ─────────────────────────────────────────────────────────────────────────────

view: dim_users {
  sql_table_name: GITHUB_PIPELINES.REPORTING_TABLES.DIM_GITHUB__USERS ;;

  # ── Primary key ──────────────────────────────────────────────────────────

  dimension: user_pk {
    primary_key: yes
    hidden:      yes
    type:        string
    sql:         ${TABLE}.USER_PK ;;
  }


  # ── Identity dimensions ───────────────────────────────────────────────────

  dimension: ldap {
    type:        string
    sql:         ${TABLE}.LDAP ;;
    label:       "LDAP"
    description: "Internal employee identifier. Join key to PR facts and commit files."
  }

  dimension: github_login {
    type:        string
    sql:         ${TABLE}.GITHUB_LOGIN ;;
    label:       "GitHub Login"
  }

  dimension: display_name {
    type:        string
    sql:         ${TABLE}.DISPLAY_NAME ;;
    label:       "Name"
  }

  dimension: email {
    type:        string
    sql:         ${TABLE}.EMAIL ;;
    label:       "Email"
  }


  # ── Org hierarchy dimensions ──────────────────────────────────────────────

  dimension: team_name {
    type:        string
    sql:         ${TABLE}.TEAM_NAME ;;
    label:       "Team"
    description: "Immediate team (e.g. Payments Backend)."
  }

  dimension: function_l1_name {
    type:        string
    sql:         ${TABLE}.FUNCTION_L1_NAME ;;
    label:       "Function"
    description: "Top-level function (e.g. Engineering, Data)."
  }

  dimension: org_name {
    type:        string
    sql:         ${TABLE}.ORG_NAME ;;
    label:       "Org"
    description: "Business unit or sub-organisation."
  }

  dimension: core_lead_ldap {
    type:        string
    sql:         ${TABLE}.CORE_LEAD ;;
    label:       "Team Lead LDAP"
    description: "LDAP of the engineer's direct manager."
  }

  dimension: employment_type {
    type:        string
    sql:         ${TABLE}.EMPLOYMENT_TYPE ;;
    label:       "Employment Type"
    description: "full_time | contractor | intern"
  }


  # ── SCD Type 2 validity dimensions ────────────────────────────────────────

  dimension: is_current {
    type:        yesno
    sql:         ${TABLE}.IS_CURRENT ;;
    label:       "Is Current Record"
    description: "TRUE for the most recent snapshot per LDAP. Always filter on this for current-state reports."
  }

  dimension_group: effective_start {
    type:        time
    timeframes:  [raw, date]
    sql:         ${TABLE}.EFFECTIVE_START_DATE ;;
    label:       "Effective Start"
    description: "Date this record version became active."
  }

  dimension_group: effective_end {
    type:        time
    timeframes:  [raw, date]
    sql:         ${TABLE}.EFFECTIVE_END_DATE ;;
    label:       "Effective End"
    description: "Date this record was superseded. 9999-12-31 = still active."
  }


  # ── Measures ──────────────────────────────────────────────────────────────

  measure: engineer_count {
    type:        count_distinct
    sql:         ${ldap} ;;
    label:       "Engineer Count"
    description: "Distinct number of engineers (current records only when is_current filter applied)."
  }
}
