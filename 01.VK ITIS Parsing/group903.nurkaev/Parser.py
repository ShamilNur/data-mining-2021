import csv
import requests
import pandas as pd
import psycopg2


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


# def file_writer(data):
#     with open('itis_kfu.csv', 'w', encoding='utf-8') as file:
#         writer = csv.writer(file)
#         for post in data:
#             writer.writerow((post['text'],))
#

all_posts = take_200_posts()

# file_writer(all_posts)

words = pd.Series([word for post in all_posts for word in post['text'].split()])
words_count = list(zip(words.value_counts().index, words.value_counts().values))

connection = psycopg2.connect(
    database='postgres',
    user='postgres',
    password='9aw25pxj',
    host='database-1.cvyhjnspp2xs.us-east-1.rds.amazonaws.com',
    port=5432)
cursor = connection.cursor()
cursor.execute('TRUNCATE TABLE vk_post')

for w in words_count:
    values = {'word': w[0], 'count': str(w[1])}
    cursor.execute('INSERT INTO vk_post (word, count) VALUES (%(word)s, %(count)s)', values)

connection.commit()
connection.close()
