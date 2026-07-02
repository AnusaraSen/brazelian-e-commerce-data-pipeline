from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

# ─────────────────────────────────────────
# Paths inside Docker container
# ─────────────────────────────────────────
DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DATA_GEN_DIR     = "/opt/airflow"
SCRIPTS_DIR      = "/opt/airflow/scripts"

sys.path.insert(0, SCRIPTS_DIR)
sys.path.insert(0, DATA_GEN_DIR)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Default arguments
# ─────────────────────────────────────────
default_args = {
    'owner':                     'data_engineering',
    'retries':                   2,
    'retry_delay':               timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'email':                     ['anusaraudaen@gmail.com'],
    'email_on_failure':          True,
    'email_on_retry':            False,
    'depends_on_past':           False,
}

# ─────────────────────────────────────────
# Task functions — ALL defined OUTSIDE the DAG block
# ─────────────────────────────────────────

def task_generate_synthetic_data(**context):
    """Generate synthetic weekly data and upload to Azure Blob."""
    from data_generation.generate_weekly_data import (
        generate_and_upload_weekly_data
    )

    logger.info("Starting synthetic data generation...")
    result = generate_and_upload_weekly_data()

    logger.info(
        f"Generated: "
        f"{result.get('orders', 0):,} orders, "
        f"{result.get('order_items', 0):,} items, "
        f"{result.get('payments', 0):,} payments, "
        f"{result.get('reviews', 0):,} reviews, "
        f"{result.get('customer_updates', 0):,} customer updates"
    )

    context['ti'].xcom_push(key='generation_result', value=result)
    return result


def task_load_bronze(**context):
    """Incrementally load new files from Azure Blob into Snowflake Bronze."""
    from data_loader.data_loader import load_incremental_only

    logger.info("Starting incremental Bronze load...")
    result = load_incremental_only()

    logger.info(
        f"Bronze load complete: "
        f"{result['tables_processed']} tables, "
        f"{result['total_rows_loaded']:,} rows loaded"
    )

    if result['errors']:
        raise Exception(f"Bronze load had errors: {result['errors']}")

    context['ti'].xcom_push(key='bronze_result', value=result)
    return result


def task_notify_success(**context):
    """Send success email with full pipeline summary."""
    from airflow.utils.email import send_email

    ti           = context['ti']
    logical_date = context.get('logical_date', context.get('execution_date'))

    generation_result = ti.xcom_pull(
        task_ids='generate_synthetic_data',
        key='generation_result'
    ) or {}

    bronze_result = ti.xcom_pull(
        task_ids='load_bronze',
        key='bronze_result'
    ) or {}

    logger.info(
        f"Pipeline completed successfully for {logical_date} | "
        f"Orders: {generation_result.get('orders', 0):,} | "
        f"Rows loaded: {bronze_result.get('total_rows_loaded', 0):,}"
    )

    subject = (
        f"✅ Pipeline Success: ecommerce_weekly_pipeline "
        f"| {str(logical_date)[:10]}"
    )

    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">

        <div style="background-color: #28a745; padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="color: white; margin: 0;">
                ✅ Weekly Pipeline Completed Successfully
            </h2>
        </div>

        <div style="background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6;">

            <p style="color: #6c757d; margin-top: 0;">
                <strong>Execution Date:</strong> {logical_date}
            </p>

            <h3 style="color: #343a40; border-bottom: 2px solid #28a745; padding-bottom: 8px;">
                Data Generation
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Orders Generated</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{generation_result.get('orders', 0):,}</strong>
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Order Items</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{generation_result.get('order_items', 0):,}</strong>
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Payments</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{generation_result.get('payments', 0):,}</strong>
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Reviews</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{generation_result.get('reviews', 0):,}</strong>
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Customer Updates (SCD2)</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{generation_result.get('customer_updates', 0):,}</strong>
                    </td>
                </tr>
            </table>

            <h3 style="color: #343a40; border-bottom: 2px solid #28a745;
                       padding-bottom: 8px; margin-top: 20px;">
                Bronze Loading
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Tables Processed</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{bronze_result.get('tables_processed', 0)}</strong>
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Total Rows Loaded</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong>{bronze_result.get('total_rows_loaded', 0):,}</strong>
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Load Errors</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong style="color: {'#dc3545' if bronze_result.get('errors') else '#28a745'};">
                            {len(bronze_result.get('errors', []))}
                        </strong>
                    </td>
                </tr>
            </table>

            <h3 style="color: #343a40; border-bottom: 2px solid #28a745;
                       padding-bottom: 8px; margin-top: 20px;">
                dbt Transformation
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Snapshot (SCD2)</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong style="color: #28a745;">✅ Complete</strong>
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Staging Models (9)</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong style="color: #28a745;">✅ Complete</strong>
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Intermediate Models (1)</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong style="color: #28a745;">✅ Complete</strong>
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">Gold Layer (5 dims + fact)</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong style="color: #28a745;">✅ Complete</strong>
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">dbt Tests (93 total)</td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: right;">
                        <strong style="color: #28a745;">✅ All Passing</strong>
                    </td>
                </tr>
            </table>

            <div style="margin-top: 20px; padding: 12px; background-color: #d4edda;
                        border-radius: 4px; border-left: 4px solid #28a745;">
                <p style="margin: 0; color: #155724;">
                    <strong>Next scheduled run:</strong> Sunday 6:00 AM UTC
                </p>
            </div>

            <p style="margin-top: 20px; color: #6c757d; font-size: 12px;">
                View full logs:
                <a href="http://localhost:8080">http://localhost:8080</a>
            </p>

        </div>

        <div style="background-color: #343a40; padding: 12px; border-radius: 0 0 8px 8px;">
            <p style="color: #adb5bd; margin: 0; font-size: 12px; text-align: center;">
                Brazilian E-Commerce Data Pipeline | Automated Weekly Run
            </p>
        </div>

    </body>
    </html>
    """

    try:
        send_email(
            to=['anusaraudaen@gmail.com'],
            subject=subject,
            html_content=html_content
        )
        logger.info("Success email sent")
    except Exception as e:
        logger.error(f"Failed to send success email: {e}")

    return "Pipeline completed successfully"


def task_notify_failure(context):
    """Send failure alert email on any task failure."""
    from airflow.utils.email import send_email

    task_instance = context['task_instance']
    exception     = context.get('exception')
    logical_date  = context.get(
        'logical_date',
        context.get('execution_date')
    )

    logger.error(
        f"PIPELINE FAILED | "
        f"Task: {task_instance.task_id} | "
        f"DAG: {task_instance.dag_id} | "
        f"Run: {logical_date} | "
        f"Error: {exception}"
    )

    subject = (
        f"🔴 Pipeline Failed: {task_instance.dag_id} "
        f"| Task: {task_instance.task_id} "
        f"| {str(logical_date)[:10]}"
    )

    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">

        <div style="background-color: #dc3545; padding: 20px; border-radius: 8px 8px 0 0;">
            <h2 style="color: white; margin: 0;">🔴 Pipeline Failure Alert</h2>
        </div>

        <div style="background-color: #f8f9fa; padding: 20px; border: 1px solid #dee2e6;">

            <div style="background-color: #f8d7da; border-left: 4px solid #dc3545;
                        padding: 12px; border-radius: 4px; margin-bottom: 20px;">
                <p style="margin: 0; color: #721c24;">
                    <strong>Action Required:</strong> The weekly pipeline has failed
                    and requires your attention.
                </p>
            </div>

            <h3 style="color: #343a40; border-bottom: 2px solid #dc3545; padding-bottom: 8px;">
                Failure Details
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; width: 35%;">
                        <strong>DAG</strong>
                    </td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        {task_instance.dag_id}
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        <strong>Failed Task</strong>
                    </td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;
                               color: #dc3545; font-weight: bold;">
                        {task_instance.task_id}
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        <strong>Execution Date</strong>
                    </td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        {logical_date}
                    </td>
                </tr>
                <tr style="background-color: #f8f9fa;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        <strong>Try Number</strong>
                    </td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        {task_instance.try_number} of 3
                    </td>
                </tr>
                <tr style="background-color: #ffffff;">
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                        <strong>Error</strong>
                    </td>
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;
                               color: #dc3545; font-family: monospace; font-size: 12px;">
                        {str(exception)[:500] if exception else 'No details available'}
                    </td>
                </tr>
            </table>

            <h3 style="color: #343a40; border-bottom: 2px solid #dc3545;
                       padding-bottom: 8px; margin-top: 20px;">
                Pipeline Task Order
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                {"".join([
                    f'''<tr style="background-color: {"#d4edda" if task != task_instance.task_id else "#f8d7da"};">
                        <td style="padding: 6px 12px; border: 1px solid #dee2e6;">
                            {"✅" if task != task_instance.task_id else "❌"} {task}
                        </td>
                    </tr>'''
                    for task in [
                        'pipeline_start',
                        'generate_synthetic_data',
                        'load_bronze',
                        'dbt_snapshot',
                        'dbt_run_staging',
                        'dbt_run_intermediate',
                        'dbt_run_gold',
                        'dbt_test',
                        'notify_success',
                        'pipeline_end'
                    ]
                ])}
            </table>

            <div style="margin-top: 20px; padding: 12px; background-color: #fff3cd;
                        border-radius: 4px; border-left: 4px solid #ffc107;">
                <p style="margin: 0; color: #856404;">
                    <strong>Note:</strong> The pipeline will retry automatically
                    (up to 2 retries with 5 minute delays).
                </p>
            </div>

            <p style="margin-top: 20px;">
                <a href="http://localhost:8080/dags/ecommerce_weekly_pipeline/grid"
                   style="background-color: #dc3545; color: white; padding: 10px 20px;
                          text-decoration: none; border-radius: 4px; display: inline-block;">
                    View in Airflow UI
                </a>
            </p>

        </div>

        <div style="background-color: #343a40; padding: 12px; border-radius: 0 0 8px 8px;">
            <p style="color: #adb5bd; margin: 0; font-size: 12px; text-align: center;">
                Brazilian E-Commerce Data Pipeline | Automated Alert
            </p>
        </div>

    </body>
    </html>
    """

    try:
        send_email(
            to=['anusaraudaen@gmail.com'],
            subject=subject,
            html_content=html_content
        )
        logger.info("Failure alert email sent")
    except Exception as e:
        logger.error(f"Failed to send failure email: {e}")


# ─────────────────────────────────────────
# DAG Definition — functions must be defined ABOVE this block
# ─────────────────────────────────────────
with DAG(
    dag_id='ecommerce_weekly_pipeline',
    description='Weekly e-commerce pipeline: generate → load → transform → test',
    default_args=default_args,
    schedule='0 6 * * 0',
    start_date=pendulum.today('UTC').subtract(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'weekly', 'production'],
    on_failure_callback=task_notify_failure,
) as dag:

    start = EmptyOperator(task_id='pipeline_start')

    generate_data = PythonOperator(
        task_id='generate_synthetic_data',
        python_callable=task_generate_synthetic_data,
        execution_timeout=timedelta(minutes=30),
    )

    load_bronze = PythonOperator(
        task_id='load_bronze',
        python_callable=task_load_bronze,
        execution_timeout=timedelta(minutes=30),
    )

    run_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt snapshot '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--target prod'
        ),
        execution_timeout=timedelta(minutes=20),
    )

    run_dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt run '
            f'--select staging.* '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--target prod'
        ),
        execution_timeout=timedelta(minutes=20),
    )

    run_dbt_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt run '
            f'--select intermediate.* '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--target prod'
        ),
        execution_timeout=timedelta(minutes=20),
    )

    run_dbt_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt run '
            f'--select marts.* '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--target prod'
        ),
        execution_timeout=timedelta(minutes=30),
    )

    run_dbt_tests = BashOperator(
        task_id='dbt_test',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            f'dbt test '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--target prod'
        ),
        execution_timeout=timedelta(minutes=20),
    )

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=task_notify_success,
        execution_timeout=timedelta(minutes=5),
    )

    end = EmptyOperator(task_id='pipeline_end')

    # ─────────────────────────────────────────
    # Pipeline dependency chain
    # ─────────────────────────────────────────
    (
        start
        >> generate_data
        >> load_bronze
        >> run_snapshot
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_gold
        >> run_dbt_tests
        >> notify_success
        >> end
    )