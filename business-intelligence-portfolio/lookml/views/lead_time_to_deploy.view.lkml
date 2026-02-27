# lead_time_to_deploy.view.lkml
# ─────────────────────────────────────────────────────────────────────────────
# View over LEAD_TIME_TO_DEPLOY — DORA lead time metrics per PR × service.
#
# Exposes lead time hours/days, DORA bucket classification, and deployment
# metadata (match scenario, staging vs production timings).
# ─────────────────────────────────────────────────────────────────────────────

view: lead_time_to_deploy {
  sql_table_name: GITHUB_PIPELINES.REPORTING_TABLES.LEAD_TIME_TO_DEPLOY ;;

  # ── Primary key ──────────────────────────────────────────────────────────

  dimension: pk {
    primary_key: yes
    hidden:      yes
    type:        string
    sql:         ${TABLE}.PK ;;
  }


  # ── Identifiers ──────────────────────────────────────────────────────────

  dimension: pull_request_id {
    type:        number
    hidden:      yes
    sql:         ${TABLE}.PULL_REQUEST_ID ;;
  }

  dimension: service {
    type:        string
    sql:         ${TABLE}.SERVICE ;;
    label:       "Service"
    description: "The service affected by this PR (from the service registry)."
  }

  dimension: org {
    type:        string
    sql:         ${TABLE}.ORG ;;
    label:       "Organisation"
  }

  dimension: repo_name {
    type:        string
    sql:         ${TABLE}.REPO_NAME ;;
    label:       "Repository"
  }


  # ── Dimensions — deployment matching ─────────────────────────────────────

  dimension: prod_match_scenario {
    type:        string
    sql:         ${TABLE}.PROD_MATCH_SCENARIO ;;
    label:       "Match Scenario"
    description: "sha_match = exact commit SHA matched to deployment record (preferred). time_after_merge = first deployment after merge time (fallback)."

    case: {
      when: { sql: ${prod_match_scenario} = 'sha_match'      ;; label: "SHA Match (exact)"    }
      when: { sql: ${prod_match_scenario} = 'time_after_merge';; label: "Time Match (fallback)"}
      else: "Unknown"
    }
  }

  dimension: is_sha_match {
    type:        yesno
    sql:         ${TABLE}.PROD_MATCH_SCENARIO = 'sha_match' ;;
    label:       "Is SHA Match"
    description: "TRUE when the deployment was matched by exact commit SHA. Use to filter for high-confidence metrics."
  }

  dimension: is_deployed {
    type:        yesno
    sql:         ${TABLE}.FIRST_PROD_DEPLOY_TIME IS NOT NULL ;;
    label:       "Is Deployed to Production"
  }

  dimension: has_staging_deployment {
    type:        yesno
    sql:         ${TABLE}.FIRST_STAGING_DEPLOY_TIME IS NOT NULL ;;
    label:       "Deployed via Staging"
    description: "TRUE when a staging deployment was recorded before production."
  }


  # ── Dimensions — DORA bucket ──────────────────────────────────────────────

  dimension: dora_lead_time_bucket {
    type:        string
    sql:         ${TABLE}.DORA_LEAD_TIME_BUCKET ;;
    label:       "DORA Lead Time Bucket"
    description: "DORA classification: elite (<1h) | high (<1d) | medium (<1w) | low (>1w)"

    order_by_field: dora_bucket_sort

    case: {
      when: { sql: ${dora_lead_time_bucket} = 'elite'  ;; label: "Elite  (< 1 hour)"  }
      when: { sql: ${dora_lead_time_bucket} = 'high'   ;; label: "High   (< 1 day)"   }
      when: { sql: ${dora_lead_time_bucket} = 'medium' ;; label: "Medium (< 1 week)"  }
      when: { sql: ${dora_lead_time_bucket} = 'low'    ;; label: "Low    (> 1 week)"  }
      else: "Unclassified"
    }
  }

  dimension: dora_bucket_sort {
    hidden:      yes
    type:        number
    sql: CASE ${TABLE}.DORA_LEAD_TIME_BUCKET
           WHEN 'elite'  THEN 1
           WHEN 'high'   THEN 2
           WHEN 'medium' THEN 3
           WHEN 'low'    THEN 4
           ELSE 5
         END ;;
  }


  # ── Dimensions — timestamps ───────────────────────────────────────────────

  dimension_group: first_commit {
    type:        time
    timeframes:  [raw, date, week, month]
    sql:         ${TABLE}.FIRST_COMMIT_DATE ;;
    label:       "First Commit"
    description: "Earliest commit in the PR — the true start of the lead time clock."
  }

  dimension_group: merged {
    type:        time
    timeframes:  [raw, date, week, month]
    sql:         ${TABLE}.MERGED_AT ;;
    label:       "Merged"
  }

  dimension_group: first_prod_deploy {
    type:        time
    timeframes:  [raw, date, week, month]
    sql:         ${TABLE}.FIRST_PROD_DEPLOY_TIME ;;
    label:       "First Production Deploy"
  }

  dimension_group: first_staging_deploy {
    type:        time
    timeframes:  [raw, date, week, month]
    sql:         ${TABLE}.FIRST_STAGING_DEPLOY_TIME ;;
    label:       "First Staging Deploy"
  }


  # ── Measures — DORA lead time ─────────────────────────────────────────────

  measure: count {
    type:        count
    label:       "Deployment Count"
    description: "Number of PR → deployment pairs. Use COUNT(DISTINCT pull_request_id) for PR count."
  }

  measure: median_lead_time_hours {
    type:        median
    label:       "Median Lead Time (hours)"
    description: "Median hours from first commit to production deployment. DORA primary metric."
    sql:         ${TABLE}.LEAD_TIME_TO_PROD_HOURS ;;
    filters:     [is_sha_match: "Yes"]
    value_format: "0.0"
  }

  measure: p95_lead_time_hours {
    type:        percentile
    percentile:  95
    label:       "P95 Lead Time (hours)"
    sql:         ${TABLE}.LEAD_TIME_TO_PROD_HOURS ;;
    filters:     [is_sha_match: "Yes"]
    value_format: "0.0"
  }

  measure: avg_merge_to_prod_hours {
    type:        average
    label:       "Avg Merge → Production (hours)"
    description: "Average hours from PR merge to first production deployment."
    sql:         ${TABLE}.MERGE_TO_PROD_HOURS ;;
    value_format: "0.0"
  }

  measure: avg_time_on_staging_hours {
    type:        average
    label:       "Avg Time on Staging (hours)"
    description: "Average hours a PR spends in staging before production. NULL for direct-to-prod services."
    sql:         ${TABLE}.TIME_ON_STAGING_HOURS ;;
    filters:     [has_staging_deployment: "Yes"]
    value_format: "0.0"
  }

  measure: pct_sha_matched {
    type:        number
    label:       "SHA Match Rate"
    description: "% of deployments matched by exact commit SHA. Target: >80% for high-confidence metrics."
    sql: COUNT(CASE WHEN ${TABLE}.PROD_MATCH_SCENARIO = 'sha_match' THEN 1 END) * 1.0
         / NULLIF(COUNT(*), 0) ;;
    value_format: "0.0%"
  }

  measure: elite_pct {
    type:        number
    label:       "Elite %"
    description: "% of deployments with lead time < 1 hour (DORA elite tier)."
    sql: COUNT(CASE WHEN ${TABLE}.DORA_LEAD_TIME_BUCKET = 'elite' THEN 1 END) * 1.0
         / NULLIF(COUNT(*), 0) ;;
    value_format: "0.0%"
  }
}
