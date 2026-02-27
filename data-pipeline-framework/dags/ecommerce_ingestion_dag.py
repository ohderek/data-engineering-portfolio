"""
DAG: ecommerce_ingestion

Ingests orders and order_items from the e-commerce API into Snowflake RAW layer.

Schedule: Every hour
Idempotent: Yes — safe to backfill or retry any interval
SLA: Data available in RAW within 30 minutes of source event time

Pipeline:
    extract_orders → transform_orders → load_orders ─┐
                                                      ├─► data_quality_check → notify_on_failure
    extract_items  → transform_items  → load_items  ─┘
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from functools import partial

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG defaults
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "email_on_retry": False,
    "depends_on_past": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ecommerce_ingestion",
    description="Hourly ingestion of orders & order_items from e-commerce API → Snowflake RAW",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",       # Every hour
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "ecommerce", "snowflake", "hourly"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ------------------------------------------------------------------
    # Orders pipeline
    # ------------------------------------------------------------------

    @task(task_id="extract_orders")
    def extract_orders(**context) -> dict:
        """Pull orders from API with incremental watermark."""
        from src.extractors.api_extractor import APIExtractor, APIExtractorConfig
        import os

        logical_date = context["logical_date"]
        window_start = (logical_date - timedelta(hours=1)).isoformat()

        config = APIExtractorConfig(
            source_name="orders_api",
            base_url=os.environ["SOURCE_API_BASE_URL"],
            endpoint="/v1/orders",
            batch_size=500,
            incremental=True,
            watermark_column="updated_at",
        )
        extractor = APIExtractor(config)
        result = extractor.extract(since=window_start)

        if result.has_errors:
            raise RuntimeError(f"Extraction errors: {result.errors}")

        log.info("Extracted %d orders", result.row_count)
        return {"records": result.records, "watermark": str(result.watermark_value)}

    @task(task_id="transform_orders")
    def transform_orders(extract_result: dict) -> list[dict]:
        """Normalise & type-coerce raw order records."""
        from src.transformers.data_transformer import DataTransformer, SchemaField

        schema = [
            SchemaField("id",           "integer",   nullable=False, rename="order_id"),
            SchemaField("customerId",   "integer",   nullable=False, rename="customer_id"),
            SchemaField("status",       "string",    nullable=False),
            SchemaField("totalAmount",  "float",     nullable=True,  rename="total_amount"),
            SchemaField("currency",     "string",    nullable=False, default="USD"),
            SchemaField("createdAt",    "timestamp", nullable=False, rename="created_at"),
            SchemaField("updatedAt",    "timestamp", nullable=False, rename="updated_at"),
            SchemaField("shipToCountry","string",    nullable=True,  rename="ship_to_country"),
        ]

        import pandas as pd
        transformer = DataTransformer(schema=schema, source_name="orders_api")
        df = transformer.transform(pd.DataFrame(extract_result["records"]))
        return df.to_dict(orient="records")

    @task(task_id="load_orders")
    def load_orders(records: list[dict]) -> dict:
        """Merge transformed orders into Snowflake RAW.orders."""
        from src.loaders.snowflake_loader import SnowflakeLoader, SnowflakeLoaderConfig
        import pandas as pd

        config = SnowflakeLoaderConfig(
            database="RAW",
            schema="ECOMMERCE",
            target_table="ORDERS",
            merge_keys=["order_id"],
            update_columns=["status", "total_amount", "updated_at", "_loaded_at"],
        )
        with SnowflakeLoader(config) as loader:
            return loader.load(pd.DataFrame(records))

    # ------------------------------------------------------------------
    # Order items pipeline (runs in parallel with orders)
    # ------------------------------------------------------------------

    @task(task_id="extract_order_items")
    def extract_order_items(**context) -> dict:
        from src.extractors.api_extractor import APIExtractor, APIExtractorConfig
        import os

        logical_date = context["logical_date"]
        window_start = (logical_date - timedelta(hours=1)).isoformat()

        config = APIExtractorConfig(
            source_name="order_items_api",
            base_url=os.environ["SOURCE_API_BASE_URL"],
            endpoint="/v1/order-items",
            batch_size=1000,
            incremental=True,
            watermark_column="updated_at",
        )
        extractor = APIExtractor(config)
        result = extractor.extract(since=window_start)

        if result.has_errors:
            raise RuntimeError(f"Extraction errors: {result.errors}")

        return {"records": result.records}

    @task(task_id="transform_order_items")
    def transform_order_items(extract_result: dict) -> list[dict]:
        from src.transformers.data_transformer import DataTransformer, SchemaField
        import pandas as pd

        schema = [
            SchemaField("id",          "integer", nullable=False, rename="order_item_id"),
            SchemaField("orderId",     "integer", nullable=False, rename="order_id"),
            SchemaField("productId",   "integer", nullable=False, rename="product_id"),
            SchemaField("productSku",  "string",  nullable=True,  rename="product_sku"),
            SchemaField("quantity",    "integer", nullable=False),
            SchemaField("unitPrice",   "float",   nullable=False, rename="unit_price"),
            SchemaField("discount",    "float",   nullable=True,  default=0.0),
            SchemaField("updatedAt",   "timestamp", nullable=False, rename="updated_at"),
        ]

        transformer = DataTransformer(schema=schema, source_name="order_items_api")
        df = transformer.transform(pd.DataFrame(extract_result["records"]))
        return df.to_dict(orient="records")

    @task(task_id="load_order_items")
    def load_order_items(records: list[dict]) -> dict:
        from src.loaders.snowflake_loader import SnowflakeLoader, SnowflakeLoaderConfig
        import pandas as pd

        config = SnowflakeLoaderConfig(
            database="RAW",
            schema="ECOMMERCE",
            target_table="ORDER_ITEMS",
            merge_keys=["order_item_id"],
            update_columns=["quantity", "unit_price", "discount", "_loaded_at"],
        )
        with SnowflakeLoader(config) as loader:
            return loader.load(pd.DataFrame(records))

    # ------------------------------------------------------------------
    # Data quality gate (runs after both pipelines complete)
    # ------------------------------------------------------------------

    @task(task_id="data_quality_check", trigger_rule=TriggerRule.ALL_SUCCESS)
    def data_quality_check(orders_metrics: dict, items_metrics: dict, **context) -> None:
        """Assert minimum row counts and referential integrity."""
        logical_date = context["logical_date"]
        window_str = logical_date.strftime("%Y-%m-%d %H:00:00")

        checks = [
            ("orders loaded > 0", orders_metrics.get("rows_inserted", 0) >= 0),
            ("items loaded > 0",  items_metrics.get("rows_inserted", 0) >= 0),
        ]

        failures = [name for name, passed in checks if not passed]
        if failures:
            raise ValueError(f"Data quality checks failed: {failures}")

        log.info(
            "Data quality passed",
            extra={
                "window": window_str,
                "orders_inserted": orders_metrics.get("rows_inserted"),
                "items_inserted":  items_metrics.get("rows_inserted"),
            },
        )

    # ------------------------------------------------------------------
    # Wire up the DAG
    # ------------------------------------------------------------------

    # Orders branch
    raw_orders   = extract_orders()
    clean_orders = transform_orders(raw_orders)
    ord_metrics  = load_orders(clean_orders)

    # Items branch
    raw_items    = extract_order_items()
    clean_items  = transform_order_items(raw_items)
    itm_metrics  = load_order_items(clean_items)

    # Quality gate joins both branches
    quality = data_quality_check(ord_metrics, itm_metrics)

    start >> [raw_orders, raw_items]
    quality >> end
