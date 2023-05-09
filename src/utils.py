import psycopg2
from decouple import config
from faker import Faker
import random


def connect():
    DATABASE_NAME = config('DATABASE_NAME')
    DATABASE_USERNAME = config('DATABASE_USERNAME')
    DATABASE_PASSWORD = config('DATABASE_PASSWORD')
    DATABASE_HOST = config('DATABASE_HOST')
    DATABASE_PORT = config('DATABASE_PORT')

    connection = psycopg2.connect(
        database=DATABASE_NAME,
        user=DATABASE_USERNAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_HOST,
        port=DATABASE_PORT
    )

    return connection


def ensure_schema(*, drop_if_exists = False):
    SCHEMA_NAME = config('SCHEMA_NAME')
    conn = connect()
    with conn.cursor() as cursor:
        if drop_if_exists:
            confirm = input(f"ARE YOU SURE WANT TO DROP SCHEMA {SCHEMA_NAME} WITH ALL OBJECTS? Type 'y' do drop...")
            if confirm == 'y':
                query = f"DROP SCHEMA if exists {SCHEMA_NAME} CASCADE"
                cursor.execute(query)
                conn.commit()

        query = f"CREATE SCHEMA if not exists {SCHEMA_NAME}"
        cursor.execute(query)
        conn.commit()

    conn.close()
    return SCHEMA_NAME

def create_tables(schema: str):
    conn = connect()
    with conn.cursor() as cursor:

        query = f"""
        CREATE TABLE if not exists {schema}.users (
            user_id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            password VARCHAR(255) NOT NULL,
            created_on TIMESTAMP NOT NULL
        );
        """
        cursor.execute(query)
        conn.commit()

        query = f"""
        CREATE TABLE if not exists {schema}.subscription_plans (
            plan_id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            plans TEXT NOT NULL
        );
        """
        cursor.execute(query)
        conn.commit()

        query = f"""
        CREATE TABLE if not exists {schema}.payments (
            payment_id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES {schema}.users(user_id),
            plan_id INTEGER NOT NULL REFERENCES {schema}.subscription_plans(plan_id),
            payment_date TIMESTAMP NOT NULL,
            amount NUMERIC(10, 2) NOT NULL
        );
        """
        cursor.execute(query)
        conn.commit()

        query = f"""
        CREATE TABLE if not exists {schema}.diaries (
            diary_id SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL REFERENCES {schema}.users(user_id)
        );
        """
        cursor.execute(query)
        conn.commit()

        query = f"""
        CREATE TABLE if not exists {schema}.diary_records (
            record_id SERIAL PRIMARY KEY,
            diary_id INTEGER NOT NULL REFERENCES {schema}.diaries(diary_id),
            title VARCHAR(255) NOT NULL,
            created_on TIMESTAMP NOT NULL,
            text TEXT NOT NULL,
            tags TEXT -- Assuming a comma-separated list of tags, you can adjust this based on your needs
        );
        """
        cursor.execute(query)
        conn.commit()

    conn.close()

    print("Tables created.")

def seed_tables(schema: str):
    fake = Faker()

    # Connect to the database
    conn = connect()

    num_users = 10
    num_plans = 3
    num_records_per_user = 50

    print(f"Generating data - {num_users} users, {num_records_per_user} records for each.")

    with conn.cursor() as cursor:
        for _ in range(num_plans):
            cursor.execute(
                f"INSERT INTO {schema}.subscription_plans (name, plans) VALUES (%s, %s)", (fake.word(), fake.text(max_nb_chars=100))
            )

        conn.commit()

        # Insert fake users, payments, diaries, and diary_records
        for _ in range(num_users):
            # Insert user
            cursor.execute(
                f"INSERT INTO {schema}.users (email, name, password, created_on) VALUES (%s, %s, %s, %s) RETURNING user_id",
                (fake.email(), fake.name(), fake.password(), fake.date_between(start_date='-1y', end_date='today'))
            )
            user_id = cursor.fetchone()[0]

            # Insert payment
            cursor.execute(
                f"INSERT INTO {schema}.payments (user_id, plan_id, payment_date, amount) VALUES (%s, %s, %s, %s)",
                (user_id, random.randint(1, num_plans), fake.date_between(start_date='-1y', end_date='today'),
                 fake.random_number(digits=2))
            )

            # Insert diary
            cursor.execute(
                f"INSERT INTO {schema}.diaries (user_id) VALUES (%s) RETURNING diary_id",
                (user_id,)
            )
            diary_id = cursor.fetchone()[0]

            # Insert diary records
            for _ in range(num_records_per_user):
                cursor.execute(
                    f"INSERT INTO {schema}.diary_records (diary_id, title, created_on, text, tags) VALUES (%s, %s, %s, %s, %s)",
                    (diary_id, fake.sentence(), fake.date_between(start_date='-1y', end_date='today'),
                     fake.text(max_nb_chars=500), ','.join(fake.words(nb=5)))
                )

        conn.commit()

    conn.close()

    print("Data generated.")