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

    return users[u_idx]


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
