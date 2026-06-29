import snowflake.connector
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Configuration
# All table loading configs in one place
# Add new tables here without touching load logic
# ─────────────────────────────────────────
TABLES_CONFIG = [
    {
        "subfolder":    "orders",
        "database":     "BRONZE_DB",
        "schema":       "RAW_ORDERS",
        "table":        "RAW_ORDERS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "order_items",
        "database":     "BRONZE_DB",
        "schema":       "RAW_ORDER_ITEMS",
        "table":        "RAW_ORDER_ITEMS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "customers",
        "database":     "BRONZE_DB",
        "schema":       "RAW_CUSTOMERS",
        "table":        "RAW_CUSTOMERS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "products",
        "database":     "BRONZE_DB",
        "schema":       "RAW_PRODUCTS",
        "table":        "RAW_PRODUCTS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "payments",
        "database":     "BRONZE_DB",
        "schema":       "RAW_ORDER_PAYMENTS",
        "table":        "RAW_ORDER_PAYMENTS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "reviews",
        "database":     "BRONZE_DB",
        "schema":       "RAW_ORDER_REVIEWS",
        "table":        "RAW_ORDER_REVIEWS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "sellers",
        "database":     "BRONZE_DB",
        "schema":       "RAW_SELLERS",
        "table":        "RAW_SELLERS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "geolocation",
        "database":     "BRONZE_DB",
        "schema":       "RAW_GEOLOCATIONS",
        "table":        "RAW_GEOLOCATIONS",
        "file_format":  "CSV"
    },
    {
        "subfolder":    "customers",   
        "database":     "BRONZE_DB",
        "schema":       "RAW_CUSTOMERS",
        "table":        "RAW_CUSTOMERS",
        "file_format":  "CSV"
    },
]

# Only synthetic tables get weekly loads
# Olist static tables (products, sellers, customers, geolocation)
# are loaded once and never change
INCREMENTAL_TABLES = [
    "orders",
    "order_items",
    "payments",
    "reviews",
    "customers"
]


def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        logger.info("Snowflake connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def check_sas_token_expiry():
    """
    Parse SAS token expiry date and warn if expiring soon.
    SAS token format: sv=...&se=2025-03-01T00:00:00Z&...
    """
    sas_token = os.getenv('AZURE_SAS_TOKEN', '')

    for part in sas_token.split('&'):
        if part.startswith('se='):
            from datetime import datetime, timezone
            expiry_str = (
                part.replace('se=', '')
                    .replace('%3A', ':')
            )
            try:
                expiry_date = datetime.fromisoformat(
                    expiry_str.replace('Z', '+00:00')
                )
                days_remaining = (
                    expiry_date - datetime.now(timezone.utc)
                ).days

                if days_remaining < 7:
                    logger.warning(
                        f"SAS token expires in {days_remaining} days. "
                        f"Regenerate in Azure Portal."
                    )
                else:
                    logger.info(
                        f"SAS token valid for {days_remaining} more days"
                    )
                return days_remaining
            except Exception as e:
                logger.warning(f"Could not parse SAS token expiry: {e}")

    return None


def load_table(cur, config, stage_name, load_type='incremental'):
    """
    Load one table from Azure stage into Snowflake Bronze.

    load_type:
        'incremental' - skip already loaded files (default)
        'full'        - force reload all files
    """
    full_table = (
        f"{config['database']}."
        f"{config['schema']}."
        f"{config['table']}"
    )
    stage_path = (
        f"@{stage_name}/"
        f"{config['subfolder']}/"
    )

    force = "TRUE" if load_type == 'full' else "FALSE"

    copy_sql = f"""
        COPY INTO {full_table}
        FROM {stage_path}
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('NULL', 'null', 'None', '')
            EMPTY_FIELD_AS_NULL = TRUE
            DATE_FORMAT = 'AUTO'
            TIMESTAMP_FORMAT = 'AUTO'
        )
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE
        FORCE = {force};
    """

    try:
        cur.execute(copy_sql)
        results = cur.fetchall()

        loaded = 0
        skipped = 0
        errors = 0
        total_rows = 0

        for row in results:
            status = row[1]
            rows_loaded = row[3] if row[3] else 0
            total_rows += rows_loaded

            if status == 'LOADED':
                loaded += 1
            elif status == 'LOAD_SKIPPED':
                skipped += 1
            else:
                errors += 1
                logger.warning(
                    f"Error in {row[0]}: {row[8] if len(row) > 8 else 'unknown'}"
                )

        logger.info(
            f"{config['table']:30s} | "
            f"Files: loaded={loaded} skipped={skipped} errors={errors} | "
            f"Rows: {total_rows:,}"
        )

        return {
            'table': config['table'],
            'files_loaded': loaded,
            'files_skipped': skipped,
            'files_errored': errors,
            'rows_loaded': total_rows
        }

    except Exception as e:
        logger.error(f"Failed to load {full_table}: {e}")
        raise


def load_all_tables(load_type='incremental'):
    """
    Main function — loads all configured tables.

    load_type:
        'incremental' — only load new files (default, used by Airflow)
        'full'        — force reload everything (use for initial setup)
    """
    # Step 1 — check SAS token before doing any work
    check_sas_token_expiry()

    stage_name = os.getenv(
        'AZURE_STAGE_NAME',
        'BRONZE_DB.RAW_ORDERS.AZURE_ECOMMERCE_STAGE'
    )

    conn = get_snowflake_connection()
    cur = conn.cursor()

    results = []
    total_rows_loaded = 0
    errors = []

    logger.info(f"Starting {load_type} load for {len(TABLES_CONFIG)} tables")
    logger.info(f"Stage: {stage_name}")
    logger.info("─" * 60)

    for config in TABLES_CONFIG:
        try:
            result = load_table(cur, config, stage_name, load_type)
            results.append(result)
            total_rows_loaded += result['rows_loaded']
        except Exception as e:
            error_msg = f"Failed to load {config['table']}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            # Continue loading other tables even if one fails
            continue

    logger.info("─" * 60)
    logger.info(f"Load complete — Total rows loaded: {total_rows_loaded:,}")

    if errors:
        logger.warning(f"{len(errors)} table(s) failed:")
        for err in errors:
            logger.warning(f"  - {err}")

    cur.close()
    conn.close()

    return {
        'tables_processed': len(results),
        'total_rows_loaded': total_rows_loaded,
        'errors': errors,
        'results': results
    }


def load_incremental_only():
    """
    Load only tables that receive new synthetic data weekly.
    Skips static tables (products, sellers, customers, geolocation)
    that never change after initial load.
    Used by Airflow for weekly synthetic data runs.
    """
    check_sas_token_expiry()

    stage_name = os.getenv(
        'AZURE_STAGE_NAME',
        'BRONZE_DB.RAW_ORDERS.AZURE_ECOMMERCE_STAGE'
    )

    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Filter to only tables that get new synthetic data
    incremental_configs = [
        cfg for cfg in TABLES_CONFIG
        if cfg['subfolder'] in INCREMENTAL_TABLES
    ]

    results = []
    total_rows = 0
    errors = []

    logger.info(
        f"Running incremental load for "
        f"{len(incremental_configs)} tables: "
        f"{[c['subfolder'] for c in incremental_configs]}"
    )

    for config in incremental_configs:
        try:
            result = load_table(cur, config, stage_name, 'incremental')
            results.append(result)
            total_rows += result['rows_loaded']
        except Exception as e:
            errors.append(str(e))
            continue

    logger.info(f"Incremental load complete — {total_rows:,} new rows")

    cur.close()
    conn.close()

    return {
        'tables_processed': len(results),
        'total_rows_loaded': total_rows,
        'errors': errors
    }


def verify_load():
    """
    Quick verification query — shows row counts and
    latest load time per Bronze table.
    """
    conn = get_snowflake_connection()
    cur = conn.cursor()

    logger.info("Verifying Bronze layer row counts:")
    logger.info("─" * 60)

    for config in TABLES_CONFIG:
        full_table = (
            f"{config['database']}."
            f"{config['schema']}."
            f"{config['table']}"
        )
        try:
            cur.execute(f"""
                SELECT
                    COUNT(*)            AS total_rows,
                    MAX(_loaded_at)     AS latest_load
                FROM {full_table}
            """)
            row = cur.fetchone()

            # Guard against None result
            if row is not None:
                logger.info(
                    f"{config['table']:30s} | "
                    f"Rows: {row[0]:>10,} | "
                    f"Latest load: {row[1]}"
                )
            else:
                logger.warning(
                    f"{config['table']:30s} | "
                    f"No data returned — table may be empty"
                )

        except Exception as e:
            logger.error(
                f"Could not verify {full_table}: {e}"
            )

    logger.info("─" * 60)
    cur.close()
    conn.close()

if __name__ == '__main__':
    import sys

    # Usage:
    # python snowflake_loader.py              → incremental load
    # python snowflake_loader.py full         → full reload
    # python snowflake_loader.py verify       → just verify counts
    # python snowflake_loader.py incremental  → incremental only tables

    mode = sys.argv[1] if len(sys.argv) > 1 else 'incremental'

    if mode == 'full':
        logger.info("Running FULL load — all files will be reloaded")
        result = load_all_tables(load_type='full')

    elif mode == 'verify':
        logger.info("Running verification only")
        verify_load()

    elif mode == 'incremental':
        logger.info("Running incremental load — new files only")
        result = load_incremental_only()

    else:
        logger.info("Running default incremental load")
        result = load_incremental_only()

    logger.info("Done")