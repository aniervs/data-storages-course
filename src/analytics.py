import matplotlib.pyplot as plt
import pandas as pd

from src.utils import connect
from src.redis_utils import TrendingTopics


def create_daily_analytics_data_mart(schema: str):
    conn = connect()
    with conn.cursor() as cursor:
        query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.daily_analytics_data_mart (
            date DATE,
            total_active_users INTEGER,
            total_diary_records INTEGER,
            average_text_length FLOAT,
            total_subscription INTEGER,
            total_revenue FLOAT,
        );
        """
        cursor.execute(query)
        conn.commit()
    conn.close()


def populate_data_mart(schema: str):
    conn = connect()
    with conn.cursor() as cursor:
        query = f"""
        INSERT INTO {schema}.daily_analytics_data_mart (
            date, 
            total_active_users, -- users with diary record today
            total_diary_records, 
            average_text_length,
            total_subscription,
            total_revenue
        )
        WITH date_table AS (
            SELECT DISTINCT COALESCE(dr.created_on, p.payment_date) AS date
            FROM psyassist.diary_records dr
            FULL JOIN psyassist.payments p ON dr.created_on = p.payment_date
        )
        SELECT 
            dt.date,
            COUNT(DISTINCT p.user_id) AS total_subscribed_users,
            COUNT(DISTINCT d.user_id) AS total_active_users,
            COUNT(DISTINCT dr.record_id) AS total_diary_records,
            AVG(LENGTH(dr.text)) AS average_text_length,
            COUNT(DISTINCT p.payment_id) AS total_subscription,
            SUM(p.amount) AS total_revenue
        FROM date_table dt
        LEFT JOIN psyassist.diary_records dr ON dt.date = dr.created_on
        LEFT JOIN psyassist.diaries d ON dr.diary_id = d.diary_id
        LEFT JOIN psyassist.payments p ON dt.date = p.payment_date
        GROUP BY dt.date;
        """
        cursor.execute(query)
        conn.commit()
    conn.close()


def visualize_data_mart(schema: str):
    conn = connect()
    query = f"SELECT * FROM {schema}.daily_analytics_data_mart;"
    df = pd.read_sql(query, conn)
    conn.close()

    # Plotting the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(df['date'], df['total_active_users'])
    plt.xlabel('Date')
    plt.ylabel('Total Active Users')
    plt.title('Daily Active Users')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

