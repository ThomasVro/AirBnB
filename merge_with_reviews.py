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

sqlite3_connection = sqlite3.connect('airbnb.db')
# On veut tous les commentaires jusqu'à décembre 2018
print("Récupération des commentaires")
query = "select listing_id, date, reviewer_id, reviewer_name, comments from '" + \
    constants.REVIEWS+"' where listing_id in common_ids"
frame_reviews = pd.read_sql_query(query, sqlite3_connection)


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
    except Exception as e:
        print(e)
    finally:
        thread_cursor.close()
        thread_connection.close()
        return inserted_rows

print("Insertion des données")
for listing_id in tqdm(listing_ids):
    connection = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root",
        db='airbnb'
    )
    cursor = connection.cursor()
    sql_request = "select listing_id, scraping_date,date,availability from " + \
        constants.CALENDARS+" where listing_id='" + \
        str(listing_id)+"' order by scraping_date,date"
    cursor.execute(sql_request)
    frame_calendar = pd.DataFrame(cursor.fetchall(), columns=[
        'listing_id', 'scraping_date', 'date', 'availability'])
    cursor.close()
    connection.close()

    merge = pd.merge(frame_calendar, frame_reviews,
                     how='left', on=['listing_id', 'date'])

    merge.replace(np.nan, "", regex=True, inplace=True)
    merge['comments'] = merge['comments'].apply(lambda x: str(x)[0:120])

    rows_to_add = [tuple(x) for x in merge.to_numpy()]

    n = 1000
    fragmented_res = [rows_to_add[x:x+n]
                      for x in range(0, len(rows_to_add), n)]
    with ThreadPoolExecutor(max_workers=130) as executor:
        results_insert = executor.map(thread_insert, fragmented_res)
