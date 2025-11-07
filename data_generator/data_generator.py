import os
import random
from datetime import datetime

import psycopg2
from faker import Faker


def generate_data() -> None:
    """
    Генерирует тестовые данные и загружает их в PostgreSQL базу данных.

    Функция создает случайные данные для трех таблиц:
    - users: данные пользователей
    - orders: данные заказов
    - user_activity: данные активностей пользователей

    Количество записей генерируется случайным образом в заданных диапазонах.

    Returns:
        None
    """
    db_config = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST", "postgres_db"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    fake = Faker()

    # Добавка в таблицу пользователей
    for _ in range(random.randint(5, 10)):
        cursor.execute(
            """
            INSERT INTO users (email, registration_date, region)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """,
            (fake.email(), fake.date_this_year(), fake.state()),
        )

    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    # Добавка в таблицу заказов
    for _ in range(random.randint(10, 20)):
        user_id = random.choice(user_ids)
        cursor.execute(
            """
            INSERT INTO orders (user_id, order_date, amount, status)
            VALUES (%s, %s, %s, %s)
        """,
            (
                user_id,
                fake.date_this_month(),
                round(random.uniform(20, 500), 2),
                random.choice(["completed", "pending", "cancelled"]),
            ),
        )

    # Добавка в таблицу активностей пользователей
    for _ in range(random.randint(50, 100)):
        user_id = random.choice(user_ids)
        cursor.execute(
            """
            INSERT INTO user_activity
                (user_id, activity_type, activity_date, duration_seconds)
            VALUES (%s, %s, %s, %s)
        """,
            (
                user_id,
                random.choice(["page_view", "product_view", "add_to_cart", "search"]),
                fake.date_time_this_month(),
                random.randint(10, 600),
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data generated successfully at {datetime.now()}")


if __name__ == "__main__":
    generate_data()
