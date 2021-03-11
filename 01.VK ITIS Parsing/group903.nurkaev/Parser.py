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


words_count = count_words(all_posts)


def execute(words_count):
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


execute(words_count)
