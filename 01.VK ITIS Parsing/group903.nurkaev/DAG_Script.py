from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# Operator; we need this to operate!
from airflow.utils.dates import days_ago

import psycopg2
import requests

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0, minute=5),
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    default_args=default_args,
    description='A DAG for my VK ITIS Group Parser',
    dag_id='vk_itis_group_dag',
    schedule_interval=timedelta(days=1)
)


def take_200_posts():
    url = 'https://api.vk.com/method/wall.get'

    count = 100
    offset = 0
    all_posts = []
    while offset < 200:
        params = {
            'access_token': '8ca06b288ca06b288ca06b28338cd6f0a788ca08ca06b28ec97b0715661c3d9a93aa7bf',
            'v': 5.93,
            'domain': 'itis_kfu',
            'count': count,
            'offset': offset
        }
        response = requests.get(url, params=params)
        data = response.json()['response']['items']
        all_posts.extend(data)
        offset += 100
    return all_posts


all_posts = take_200_posts()


def count_words(all_posts):
    words = []
    for post in all_posts:
        for word in post['text'].split():
            words.append(word)

    words_count = {}
    for word in words:
        words_count[word] = words_count.get(word, 0) + 1
    return words_count


def execute():
    words_count = count_words(all_posts)

    connection = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='9aw25pxj',
        host='database-1.cvyhjnspp2xs.us-east-1.rds.amazonaws.com',
        port=5432)
    cursor = connection.cursor()
    cursor.execute('TRUNCATE TABLE vk_post')

    for word in words_count.keys():
        values = {'word': word, 'count': str(words_count[word])}
        cursor.execute('INSERT INTO vk_post (word, count) VALUES (%(word)s, %(count)s)', values)

    connection.commit()
    connection.close()


with dag:
    run_this_task = PythonOperator(
        task_id='run_this',
        python_callable=execute,
        provide_context=True
    )

run_this_task
