import bcrypt
import psycopg2
from decouple import config
from faker import Faker
import random
import datetime
from dataclasses import dataclass

from src.utils import connect
from src.redis_utils import TrendingTopics


@dataclass
class User:
    id: int
    email: str
    name: str


def interactive_login(conn, schema):
    with conn.cursor() as cursor:
        cursor.execute(f"select user_id, email, name from {schema}.users order by email")
        users = [User(*u) for u in cursor.fetchall()]

    for i, u in enumerate(users):
        print(f"{i}: {u.email}\t{u.name}")

    u_idx = int(input("To login, type user number: "))

    # insert record to user_activity table
    with conn.cursor() as cursor:
        dt = datetime.datetime.now()
        cursor.execute(f"INSERT INTO {schema}.user_activity (user_id, activity_type, activity_date) VALUES (%s, %s, %s)",
                       (users[u_idx].id, 'login', dt))
        recalc_actions_datamart(conn, schema, users[u_idx].id, dt)

        conn.commit()

    return users[u_idx]


def recalc_actions_datamart(conn, schema, user_id, date):
    # select all records from user_activity table for the user_id and date
    # insert records to actions_datamart table with corresponding aggregations

    date_str = date.strftime('%Y-%m-%d')

    with conn.cursor() as cursor:
        cursor.execute(
            f"""DELETE FROM {schema}.user_activity_datamart WHERE date(activity_date) = date(%s)""",
            (date_str, ))

        cursor.execute(
            f"""
            INSERT INTO {schema}.user_activity_datamart (activity_type, activity_date, activity_hour, activity_count)
            (SELECT activity_type, date(activity_date), EXTRACT(HOUR FROM activity_date) as activity_hour, COUNT(*) as activity_count
            FROM {schema}.user_activity
            WHERE date(activity_date) = date(%s)
            GROUP BY activity_type, date(activity_date), EXTRACT(HOUR FROM activity_date)
            ORDER BY date(activity_date), activity_hour)
            """
            , (date_str,))


def add_diary_records(conn, schema, user):

    trending_monitor = TrendingTopics(host=config('REDIS_HOST'), port=config('REDIS_PORT'), password=config('REDIS_PASSWORD'))

    diary_id = None
    text = '<start>'

    while text:
        text = input(f"Please, write your diary record (or empty line to finish):\n")
        if text.strip() == '':
            break

        words = text.split(' ')

        if not words or (len(words) < 3):
            print("Please, three words or more!")
            continue

        trending_monitor.update_trending(text)

        with conn.cursor() as cursor:
            if diary_id is None:
                cursor.execute(f"SELECT diary_id from {schema}.diaries where user_id = %s", (user.id,))
                diary_id = cursor.fetchone()

            title = ' '.join(words[:2])
            tags = ','.join(words[:2])

            cursor.execute(
                f"INSERT INTO {schema}.diary_records (diary_id, title, created_on, text, tags) VALUES (%s, %s, %s, %s, %s)",
                (diary_id, title, datetime.datetime.now(), text, tags))

            dt = datetime.datetime.now()

            # insert record to user_activity table
            cursor.execute(f"INSERT INTO {schema}.user_activity (user_id, activity_type, activity_date) VALUES (%s, %s, %s)",
                            (user.id, 'diary_record', dt))

            recalc_actions_datamart(conn, schema, user.id, dt)

            conn.commit()

            trending = trending_monitor.get_trending()
            if trending:
                print("\nTrending now:\n")
                for i, w in enumerate(trending):
                    print(f"{i}. {w.decode()}")
            else:
                print("It's calm in the whole world now!")

            print()

def start_interaction(schema):
    conn = connect()

    user = interactive_login(conn, schema)
    print(f"Logged in as {user.name}")

    add_diary_records(conn, schema, user)

    # select and print hourly activity of all users from the user_activity_datamart table for the current day
    # grouped by hour and activity type
    with conn.cursor() as cursor:
        cursor.execute(
            f"""SELECT activity_hour, activity_type, activity_count FROM {schema}.user_activity_datamart
            WHERE date(activity_date) = date(%s)
            ORDER BY activity_hour, activity_type""",
            (datetime.datetime.now().strftime('%Y-%m-%d'), ))

        print("\nHourly activity:\nhour\ttype\tcount\n")
        for row in cursor.fetchall():
            print(f"{row[0]}\t{row[1]}\t{row[2]}")
