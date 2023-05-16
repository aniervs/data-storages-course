import bcrypt
import psycopg2
from decouple import config
from faker import Faker
import random
import datetime


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


def ensure_schema(*, drop_if_exists=False):
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


def drop_tables(schema: str):
    conn = connect()
    with conn.cursor() as cur:
        cur.execute(f"""DROP TABLE IF EXISTS {schema}.users CASCADE;""")
        cur.execute(f"""DROP TABLE IF EXISTS {schema}.payments CASCADE ;""")
        cur.execute(f"""DROP TABLE IF EXISTS {schema}.subscription_plans CASCADE;""")
        cur.execute(f"""DROP TABLE IF EXISTS {schema}.diaries CASCADE ;""")
        cur.execute(f"""DROP TABLE IF EXISTS {schema}.diary_records CASCADE;""")

        # Commit the changes
        conn.commit()

    # Close the connection to the PostgreSQL database
    conn.close()



def create_tables(schema: str):
    conn = connect()
    with conn.cursor() as cursor:
        query = f"""
        CREATE TABLE if not exists {schema}.users (
            user_id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            balance FLOAT,
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
                plans TEXT NOT NULL,
                price DECIMAL(10,2) NOT NULL
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
        # Insert fake subscription plans with random price
        for _ in range(num_plans):
            name = fake.word()
            plans = fake.text(max_nb_chars=100)
            price = round(random.uniform(1, 100), 2)
            cursor.execute(
                f"INSERT INTO {schema}.subscription_plans (name, plans, price) VALUES (%s, %s, %s)",
                (name, plans, price)
            )

        conn.commit()

        # Insert fake users, payments, diaries, and diary_records
        for _ in range(num_users):
            # Insert user
            salt = bcrypt.gensalt()
            email = fake.email()
            open_pwd = fake.password()
            balance = fake.random.uniform(0, 10000)
            pwd = bcrypt.hashpw(open_pwd.encode(), salt).decode()
            cursor.execute(
                f"INSERT INTO {schema}.users (email, name, password, balance, created_on) VALUES (%s, %s, %s, %s, %s) RETURNING user_id",
                (email, fake.name(), pwd, balance, fake.date_between(start_date='-1y', end_date='today'))
            )
            user_id = cursor.fetchone()[0]

            print(f"Created user {user_id} {email} with pwd {open_pwd}")

            # Select random subscription plan
            cursor.execute(f"SELECT plan_id FROM {schema}.subscription_plans ORDER BY RANDOM() LIMIT 1")
            plan_id = cursor.fetchone()[0]

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


def pay_subscription(user_id: int, plan_id: int, payment_date: datetime.date) -> bool:
    # establish the database connection and begin transaction
    conn = None
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
        cursor.execute('BEGIN TRANSACTION')

        # get the price of the subscription plan
        sql = 'SELECT price FROM subscription_plans WHERE plan_id = %s'
        cursor.execute(sql, (plan_id,))

        amount = cursor.fetchone()[0]
        # check if the plan_id exists in the subscription_plans table
        if amount is None:
            print(f'Plan with ID {plan_id} does not exist.')
            raise ValueError(f'Plan with ID {plan_id} does not exist.')

        # get the balance of the user
        sql = 'SELECT balance FROM subscription_plans WHERE user_id = %s'
        cursor.execute(sql, (user_id,))

        balance = cursor.fetchone()[0]
        # check if the user exists
        if balance is None:
            print(f'User with ID {user_id} does not exist.')
            raise ValueError(f'User with ID {user_id} does not exist.')

        assert balance >= amount, f'User with ID {user_id} does not have enough balance for this transaction to happen'

        # insert the user transaction data into the database
        sql = 'INSERT INTO user_transactions (user_id, plan_id, payment_date, amount) VALUES (%s, %s, %s, %s)'
        cursor.execute(sql, (user_id, plan_id, payment_date, amount))

        # update the balance of the user
        balance -= amount
        sql = 'UPDATE users SET balance = %s WHERE user_id = %s'
        cursor.execute(sql, (balance, user_id))

        # commit the transaction
        conn.commit()
    except Exception as error:
        # rollback the transaction if an error occurs
        if conn:
            conn.rollback()
        print(str(error))
    finally:
        # close the database connection
        if conn:
            conn.close()

