# dora_metrics.dashboard.lkml
# ─────────────────────────────────────────────────────────────────────────────
# DORA Metrics dashboard — engineering leadership view.
#
# Tiles:
#   1. KPI row: Median lead time / P95 lead time / Elite % / SHA match rate
#   2. Lead time trend (weekly median, line chart)
#   3. DORA bucket distribution (donut)
#   4. Lead time by service (bar, top 20)
#   5. Lead time by team (bar, current snapshot)
#   6. Staging vs production time breakdown (stacked bar)
#
# Default filters: last 90 days, production SHA matches only
# ─────────────────────────────────────────────────────────────────────────────

- dashboard: dora_metrics
  title: "DORA Metrics — Engineering Velocity"
  layout: newspaper
  preferred_viewer: dashboards-next
  description: "Lead time to deploy, deployment frequency, and DORA bucket classification by service and team."

  filters:

  - name: date_range
    title: "Date Range"
    type: date_filter
    default_value: "90 days"
    explore: dora_lead_time
    field: lead_time_to_deploy.first_prod_deploy_date

  - name: org_filter
    title: "Organisation"
    type: field_filter
    explore: dora_lead_time
    field: lead_time_to_deploy.org
    default_value: ""

  - name: service_filter
    title: "Service"
    type: field_filter
    explore: dora_lead_time
    field: lead_time_to_deploy.service
    default_value: ""


  elements:

  # ── KPI row ────────────────────────────────────────────────────────────────

  - title: "Median Lead Time (hours)"
    name: kpi_median_lead_time
    model: engineering_velocity
    explore: dora_lead_time
    type: single_value
    fields: [lead_time_to_deploy.median_lead_time_hours]
    custom_color_enabled: true
    show_single_value_title: true
    single_value_title: "Median Lead Time"
    value_format: "0.0\"h\""
    width: 3
    height: 3

  - title: "P95 Lead Time (hours)"
    name: kpi_p95_lead_time
    model: engineering_velocity
    explore: dora_lead_time
    type: single_value
    fields: [lead_time_to_deploy.p95_lead_time_hours]
    show_single_value_title: true
    single_value_title: "P95 Lead Time"
    value_format: "0.0\"h\""
    width: 3
    height: 3

  - title: "Elite Deployment %"
    name: kpi_elite_pct
    model: engineering_velocity
    explore: dora_lead_time
    type: single_value
    fields: [lead_time_to_deploy.elite_pct]
    show_single_value_title: true
    single_value_title: "Elite % (< 1 hour)"
    value_format: "0.0%"
    width: 3
    height: 3

  - title: "SHA Match Rate"
    name: kpi_sha_match_rate
    model: engineering_velocity
    explore: dora_lead_time
    type: single_value
    fields: [lead_time_to_deploy.pct_sha_matched]
    show_single_value_title: true
    single_value_title: "SHA Match Rate"
    value_format: "0.0%"
    note_display: above
    note_state: collapsed
    note_text: "Target: >80%. Low match rate = deployment tools not capturing commit SHAs."
    width: 3
    height: 3


  # ── Lead time trend ────────────────────────────────────────────────────────

  - title: "Lead Time Trend (weekly)"
    name: lead_time_trend
    model: engineering_velocity
    explore: dora_lead_time
    type: looker_line
    fields:
      - lead_time_to_deploy.first_prod_deploy_week
      - lead_time_to_deploy.median_lead_time_hours
      - lead_time_to_deploy.p95_lead_time_hours
    sorts: [lead_time_to_deploy.first_prod_deploy_week asc]
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    point_style: circle_outline
    series_labels:
      lead_time_to_deploy.median_lead_time_hours: "Median"
      lead_time_to_deploy.p95_lead_time_hours: "P95"
    reference_lines:
      - reference_type: line
        line_value: "24"
        label: "1 day (high tier)"
        color: "#F0B429"
      - reference_type: line
        line_value: "1"
        label: "1 hour (elite tier)"
        color: "#27AB83"
    width: 12
    height: 5


  # ── DORA bucket distribution ───────────────────────────────────────────────

  - title: "DORA Bucket Distribution"
    name: dora_bucket_dist
    model: engineering_velocity
    explore: dora_lead_time
    type: looker_donut_multiples
    fields:
      - lead_time_to_deploy.dora_lead_time_bucket
      - lead_time_to_deploy.count
    sorts: [lead_time_to_deploy.dora_bucket_sort asc]
    color_application:
      collection_id: custom
      custom_colors:
        - id: elite
          color: "#27AB83"
        - id: high
          color: "#3B82F6"
        - id: medium
          color: "#F0B429"
        - id: low
          color: "#E53E3E"
    width: 6
    height: 5


  # ── Lead time by service ───────────────────────────────────────────────────

  - title: "Median Lead Time by Service (Top 20)"
    name: lead_time_by_service
    model: engineering_velocity
    explore: dora_lead_time
    type: looker_bar
    fields:
      - lead_time_to_deploy.service
      - lead_time_to_deploy.median_lead_time_hours
      - lead_time_to_deploy.count
    sorts: [lead_time_to_deploy.median_lead_time_hours asc]
    limit: 20
    x_axis_label: "Median Lead Time (hours)"
    show_value_labels: true
    label_density: 25
    color_application:
      collection_id: custom
      custom_colors:
        - color: "#3B82F6"
    width: 6
    height: 5


  # ── Lead time by team ──────────────────────────────────────────────────────

  - title: "Median Lead Time by Team"
    name: lead_time_by_team
    model: engineering_velocity
    explore: dora_lead_time
    type: looker_bar
    fields:
      - dim_users.team_name
      - lead_time_to_deploy.median_lead_time_hours
      - lead_time_to_deploy.count
    filters:
      dim_users.is_current: "Yes"
    sorts: [lead_time_to_deploy.median_lead_time_hours asc]
    limit: 25
    x_axis_label: "Median Lead Time (hours)"
    width: 6
    height: 6


  # ── Staging vs production breakdown ───────────────────────────────────────

  - title: "Merge → Staging → Production Time Breakdown"
    name: staging_prod_breakdown
    model: engineering_velocity
    explore: dora_lead_time
    type: looker_bar
    fields:
      - lead_time_to_deploy.service
      - lead_time_to_deploy.avg_merge_to_prod_hours
      - lead_time_to_deploy.avg_time_on_staging_hours
    filters:
      lead_time_to_deploy.has_staging_deployment: "Yes"
    sorts: [lead_time_to_deploy.avg_merge_to_prod_hours desc]
    limit: 15
    stacking: normal
    series_labels:
      lead_time_to_deploy.avg_merge_to_prod_hours: "Merge → Production"
      lead_time_to_deploy.avg_time_on_staging_hours: "Time on Staging"
    note_display: above
    note_state: collapsed
    note_text: "Services with staging deployments only. Long 'Time on Staging' bars indicate manual promotion bottlenecks."
    width: 6
    height: 6
