import pandas as pd
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
import snowflake.connector
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import io
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

def fetch_existing_ids():
    """
    Fetch real IDs from Snowflake dimensions so synthetic
    orders reference valid existing entities.
    This keeps your star schema joins intact.
    """
    conn = get_snowflake_connection()
    cur = conn.cursor()

    logger.info("Fetching existing customer IDs...")
    cur.execute("""
        SELECT customer_id, customer_unique_id, zip_code_prefix
        FROM SILVER_DB.PUBLIC_STAGING.STG_CUSTOMERS
        ORDER BY RANDOM()
        LIMIT 5000
    """)
    customers = pd.DataFrame(
        cur.fetchall(),
        columns=['customer_id', 'customer_unique_id', 'zip_code_prefix']
    )

    logger.info("Fetching existing product IDs...")
    cur.execute("""
        SELECT product_id, product_category_name
        FROM SILVER_DB.PUBLIC_STAGING.STG_PRODUCTS
        ORDER BY RANDOM()
        LIMIT 2000
    """)
    products = pd.DataFrame(
        cur.fetchall(),
        columns=['product_id', 'product_category_name']
    )

    logger.info("Fetching existing seller IDs...")
    cur.execute("""
        SELECT seller_id, seller_zip_code_prefix
        FROM SILVER_DB.PUBLIC_STAGING.STG_SELLERS
        ORDER BY RANDOM()
    """)
    sellers = pd.DataFrame(
        cur.fetchall(),
        columns=['seller_id', 'seller_zip_code_prefix']
    )

    cur.close()
    conn.close()

    logger.info(f"Fetched {len(customers)} customers, "
                f"{len(products)} products, "
                f"{len(sellers)} sellers")

    return customers, products, sellers

def generate_orders(customers, n_orders):
    """Generate synthetic order headers."""
    today = datetime.utcnow()
    
    # Random purchase timestamps within the last 7 days
    purchase_timestamps = [
        today - timedelta(
            days=random.uniform(0, 7),
            hours=random.uniform(0, 23),
            minutes=random.uniform(0, 59)
        )
        for _ in range(n_orders)
    ]

    orders = pd.DataFrame({
        'order_id': [str(uuid.uuid4()) for _ in range(n_orders)],
        'customer_id': customers['customer_id'].sample(
            n_orders, replace=True
        ).values,
        'order_status': np.random.choice(
            ['delivered', 'shipped', 'delivered', 'delivered'],
            # Weighted toward delivered to match Olist distribution
            n_orders
        ),
        'order_purchase_timestamp': purchase_timestamps,
        'order_approved_at': [
            ts + timedelta(hours=random.uniform(0.5, 2))
            for ts in purchase_timestamps
        ],
        'order_delivered_carrier_date': [
            ts + timedelta(days=random.uniform(1, 3))
            for ts in purchase_timestamps
        ],
        'order_delivered_customer_date': [
            ts + timedelta(days=random.uniform(5, 15))
            for ts in purchase_timestamps
        ],
        'order_estimated_delivery_date': [
            ts + timedelta(days=random.uniform(10, 20))
            for ts in purchase_timestamps
        ],
        '_file_name': f'synthetic_orders_{today.strftime("%Y%m%d")}.csv',
        '_loaded_at': today
    })

    return orders

def generate_order_items(orders, products, sellers):
    """
    Generate order items. Each order gets 1-4 items.
    Realistic distribution — most orders have 1-2 items.
    """
    items = []
    
    for _, order in orders.iterrows():
        # Weighted: 60% 1 item, 25% 2 items, 10% 3 items, 5% 4 items
        n_items = np.random.choice(
            [1, 2, 3, 4],
            p=[0.60, 0.25, 0.10, 0.05]
        )
        
        for item_seq in range(1, n_items + 1):
            product = products.sample(1).iloc[0]
            seller = sellers.sample(1).iloc[0]
            
            # Price distribution similar to Olist
            # Most items between R$20 and R$500
            price = round(np.random.lognormal(4.5, 0.8), 2)
            price = max(9.90, min(price, 6735.00))  # Olist min/max
            freight = round(random.uniform(8.0, 60.0), 2)

            items.append({
                'order_id': order['order_id'],
                'order_item_id': item_seq,
                'product_id': product['product_id'],
                'seller_id': seller['seller_id'],
                'shipping_limit_date': (
                    order['order_purchase_timestamp'] +
                    timedelta(days=random.uniform(2, 5))
                ),
                'price': price,
                'freight_value': freight,
                '_file_name': (
                    f'synthetic_order_items_'
                    f'{datetime.utcnow().strftime("%Y%m%d")}.csv'
                ),
                '_loaded_at': datetime.utcnow()
            })

    return pd.DataFrame(items)

def generate_payments(orders):
    """
    Generate payments. Most orders have one payment.
    ~15% have mixed payments (voucher + credit card).
    """
    payments = []

    for _, order in orders.iterrows():
        is_mixed = random.random() < 0.15

        if is_mixed:
            # Voucher covers part, credit card covers rest
            total = round(random.uniform(50, 800), 2)
            voucher_amount = round(total * random.uniform(0.1, 0.4), 2)
            card_amount = round(total - voucher_amount, 2)

            payments.append({
                'order_id': order['order_id'],
                'payment_sequential': 1,
                'payment_type': 'voucher',
                'payment_installments': 1,
                'payment_value': voucher_amount,
                '_file_name': (
                    f'synthetic_payments_'
                    f'{datetime.utcnow().strftime("%Y%m%d")}.csv'
                ),
                '_loaded_at': datetime.utcnow()
            })
            payments.append({
                'order_id': order['order_id'],
                'payment_sequential': 2,
                'payment_type': 'credit_card',
                'payment_installments': random.choice([1, 2, 3, 6, 12]),
                'payment_value': card_amount,
                '_file_name': (
                    f'synthetic_payments_'
                    f'{datetime.utcnow().strftime("%Y%m%d")}.csv'
                ),
                '_loaded_at': datetime.utcnow()
            })
        else:
            payment_type = np.random.choice(
                ['credit_card', 'boleto', 'debit_card'],
                p=[0.74, 0.19, 0.07]
            )
            payments.append({
                'order_id': order['order_id'],
                'payment_sequential': 1,
                'payment_type': payment_type,
                'payment_installments': (
                    random.choice([1, 2, 3, 6, 12])
                    if payment_type == 'credit_card' else 1
                ),
                'payment_value': round(random.uniform(20, 800), 2),
                '_file_name': (
                    f'synthetic_payments_'
                    f'{datetime.utcnow().strftime("%Y%m%d")}.csv'
                ),
                '_loaded_at': datetime.utcnow()
            })

    return pd.DataFrame(payments)

def generate_reviews(orders):
    """Generate reviews. Not every order gets a review."""
    reviews = []

    for _, order in orders.iterrows():
        # 80% of delivered orders get a review
        if (order['order_status'] == 'delivered' and
                random.random() < 0.80):

            creation_date = (
                order['order_delivered_customer_date'] +
                timedelta(days=random.uniform(1, 7))
            )

            # Olist score distribution: heavily skewed to 5 stars
            score = np.random.choice(
                [1, 2, 3, 4, 5],
                p=[0.05, 0.03, 0.08, 0.14, 0.70]
            )

            reviews.append({
                'review_id': str(uuid.uuid4()),
                'order_id': order['order_id'],
                'review_score': score,
                'review_comment_title': None,
                'review_comment_message': None,
                'review_creation_date': creation_date,
                'review_answer_timestamp': (
                    creation_date +
                    timedelta(hours=random.uniform(1, 72))
                ),
                '_file_name': (
                    f'synthetic_reviews_'
                    f'{datetime.utcnow().strftime("%Y%m%d")}.csv'
                ),
                '_loaded_at': datetime.utcnow()
            })

    return pd.DataFrame(reviews)

def upload_to_azure(df, blob_name):
    """Upload DataFrame as CSV to Azure Blob Storage."""
    account = os.getenv('AZURE_STORAGE_ACCOUNT')
    sas_token = os.getenv('AZURE_SAS_TOKEN')
    container = os.getenv('AZURE_CONTAINER_NAME')

    # Validate required environment variables to avoid passing None
    if not account:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT environment variable is not set")
    if not container:
        raise RuntimeError("AZURE_CONTAINER_NAME environment variable is not set")

    account_url = f"https://{account}.blob.core.windows.net"
    client = BlobServiceClient(
        account_url=account_url,
        credential=sas_token
    )

    # Convert to CSV in memory — no temp files needed
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    container_client = client.get_container_client(container)
    container_client.upload_blob(
        name=blob_name,
        data=csv_bytes,
        overwrite=True
    )
    logger.info(f"Uploaded {len(df)} rows → {blob_name}")

def generate_and_upload_weekly_data(n_orders=None):
    """
    Main function called by Airflow.
    Generates between 500 and 2000 orders randomly
    to simulate realistic weekly volume variation.
    """
    if n_orders is None:
        n_orders = random.randint(500, 2000)

    today = datetime.utcnow().strftime('%Y%m%d')
    logger.info(f"Generating {n_orders} synthetic orders for {today}")

    # Step 1 — fetch real IDs from Snowflake
    customers, products, sellers = fetch_existing_ids()

    # Step 2 — generate all four datasets
    orders = generate_orders(customers, n_orders)
    order_items = generate_order_items(orders, products, sellers)
    payments = generate_payments(orders)
    reviews = generate_reviews(orders)

    logger.info(
        f"Generated: {len(orders)} orders, "
        f"{len(order_items)} items, "
        f"{len(payments)} payments, "
        f"{len(reviews)} reviews"
    )

    # Step 3 — upload to Azure Blob
    upload_to_azure(orders,      f'orders/synthetic_orders_{today}.csv')
    upload_to_azure(order_items, f'order_items/synthetic_items_{today}.csv')
    upload_to_azure(payments,    f'payments/synthetic_payments_{today}.csv')
    upload_to_azure(reviews,     f'reviews/synthetic_reviews_{today}.csv')

    return {
        'orders': len(orders),
        'order_items': len(order_items),
        'payments': len(payments),
        'reviews': len(reviews),
        'date': today
    }

if __name__ == '__main__':
    result = generate_and_upload_weekly_data()
    print(f"Weekly generation complete: {result}")