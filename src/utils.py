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

    return connection.cursor()


def create_tables():
    query = """
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    created_on TIMESTAMP NOT NULL
);

CREATE TABLE subscription_plans (
    plan_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    plans TEXT NOT NULL
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(user_id),
    plan_id INTEGER NOT NULL REFERENCES subscription_plans(plan_id),
    payment_date TIMESTAMP NOT NULL,
    amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE diaries (
    diary_id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE NOT NULL REFERENCES users(user_id)
);

CREATE TABLE diary_records (
    record_id SERIAL PRIMARY KEY,
    diary_id INTEGER NOT NULL REFERENCES diaries(diary_id),
    title VARCHAR(255) NOT NULL,
    created_on TIMESTAMP NOT NULL,
    text TEXT NOT NULL,
    tags TEXT -- Assuming a comma-separated list of tags, you can adjust this based on your needs
);
"""
    cursor = connect()
    cursor.execute(query)
    cursor.execute("COMMIT")


def seed_tables():
    fake = Faker()

    # Connect to the database
    cursor = connect()

    num_users = 10
    num_plans = 3
    num_records_per_user = 5

    for _ in range(num_plans):
        cursor.execute(
            "INSERT INTO subscription_plans (name, plans) VALUES (%s, %s)", (fake.word(), fake.text(max_nb_chars=100))
        )

    # Insert fake users, payments, diaries, and diary_records
    for _ in range(num_users):
        # Insert user
        cursor.execute(
            "INSERT INTO users (email, name, password, created_on) VALUES (%s, %s, %s, %s)",
            (fake.email(), fake.name(), fake.password(), fake.date_between(start_date='-1y', end_date='today'))
        )
        user_id = cursor.fetchone()[0]

        # Insert payment
        cursor.execute(
            "INSERT INTO payments (user_id, plan_id, payment_date, amount) VALUES (%s, %s, %s, %s)",
            (user_id, random.randint(1, num_plans), fake.date_between(start_date='-1y', end_date='today'), fake.random_number(digits=2))
        )

        # Insert diary
        cursor.execute(
            "INSERT INTO diaries (user_id) VALUES (%s)",
            (user_id,)
        )
        diary_id = cursor.fetchone()[0]

        # Insert diary records
        for _ in range(num_records_per_user):
            cursor.execute(
                "INSERT INTO diary_records (diary_id, title, created_on, text, tags) VALUES (%s, %s, %s, %s, %s)",
                (diary_id, fake.sentence(), fake.date_between(start_date='-1y', end_date='today'), fake.text(max_nb_chars=500), ','.join(fake.words(nb=5)))
            )

    cursor.execute("COMMIT")
