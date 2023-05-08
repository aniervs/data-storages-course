import psycopg2
from decouple import config


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
    connection = connect()
    connection.execute(query)
    connection.execute("COMMIT")

