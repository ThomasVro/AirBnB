import pandas as pd
import sqlite3
import pymysql.cursors
from concurrent.futures import ThreadPoolExecutor
import constants
import numpy as np
from tqdm import tqdm


def get_items_list(sql_request):
    connection = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root",
        db='airbnb'
    )
    cursor = connection.cursor()
    cursor.execute(sql_request)
    cursor.close()
    connection.close()
    return [x[0] for x in cursor.fetchall()]


print("Récupération des listing_id")
# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.CALENDARS)


def thread_insert(rows_to_add):
    inserted_rows = 0
    try:
        thread_connection = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="root",
            db='airbnb'
        )
        thread_cursor = thread_connection.cursor()
        sql_insert = "insert into "+constants.AIRBNB + \
            " (listing_id,scraping_date,date,availability,reviewer_id, reviewer_name, comments) values (%s,%s,%s,%s,%s,%s,%s)"
        inserted_rows = thread_cursor.executemany(sql_insert, rows_to_add)
        thread_connection.commit()
        thread_cursor.close()
        thread_connection.close()
    except Exception as e:
        print(e)
    finally:
        return inserted_rows


print("Insertion des données pour",len(listing_ids),"annonces")
inserted_rows = 0
connection = pymysql.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="root",
    db='airbnb'
)
cursor = connection.cursor()
for listing_id in tqdm(listing_ids):
    sql_request = "select a.listing_id,a.scraping_date,a.date,a.availability,b.reviewer_id,b.reviewer_name,b.comments from calendars_2018 as a left join reviews_2018 as b on a.listing_id = b.listing_id and a.date = b.date where a.listing_id=" + \
        str(listing_id)+" order by scraping_date,date"
    cursor.execute(sql_request)
    rows_to_add = cursor.fetchall()

    n = 1000
    fragmented_res = [rows_to_add[x:x+n]
                      for x in range(0, len(rows_to_add), n)]
    with ThreadPoolExecutor(max_workers=130) as executor:
        results_insert = executor.map(thread_insert, fragmented_res)

    for r in results_insert:
        inserted_rows+=r
print(inserted_rows,"lignes insérées")
cursor.close()
connection.close()
