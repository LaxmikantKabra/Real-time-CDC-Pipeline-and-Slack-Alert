import random
import psycopg2
import faker
from datetime import datetime

fake = faker.Faker()


def generate_transaction():
    user = fake.simple_profile()
    choices = ["SUCCESS", "FAILED"]
    weights = [19, 1]
    return {
        "transactionId": fake.uuid4(),
        "userId": user["username"],
        "timestamp": datetime.now().timestamp(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(["USD"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(
            ["credit_card", "debit_card", "online_transfer"]
        ),
        "orderStatus": random.choices(choices, weights=weights)[0],
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(["", "DISCOUNT10", ""]),
        "affiliateId": fake.uuid4(),
    }


def cancel_transaction(cursor, tid="random"):
    if tid == "random":
        cursor.execute("SELECT cancel_random_transaction()")
    else:
        cursor.execute(
            """UPDATE transactions SET order_status = 'CANCELLED'
            WHERE transaction_id = %s
            """,
            (tid,),
        )


if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        database="financial_db",
        user="postgres",
        password="postgres",
        port=5432,
    )

    cur = conn.cursor()

    for i in range(200000):
        transaction = generate_transaction()
        try:
            cur.execute(
                """
                INSERT INTO transactions(transaction_id, user_id, timestamp, amount,
                currency, city, country, merchant_name, payment_method, order_status, 
                ip_address, affiliateId, voucher_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    transaction["transactionId"],
                    transaction["userId"],
                    datetime.fromtimestamp(transaction["timestamp"]).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    transaction["amount"],
                    transaction["currency"],
                    transaction["city"],
                    transaction["country"],
                    transaction["merchantName"],
                    transaction["paymentMethod"],
                    transaction["orderStatus"],
                    transaction["ipAddress"],
                    transaction["affiliateId"],
                    transaction["voucherCode"],
                ),
            )
        except psycopg2.errors.UniqueViolation:
            conn.rollback()  # Rollback the transaction on error
            print(
                f"Duplicate transaction_id found: {transaction['transactionId']}. Skipping insert."
            )
            continue

    cur.close()
    conn.commit()
    conn.close()
