import sqlite3
import pymysql.cursors
from functools import reduce
import numpy as np
import constants
from concurrent.futures import ThreadPoolExecutor
import time
from tqdm import tqdm

start = time.time()

conn = sqlite3.connect('airbnb.db')
c = conn.cursor()

def thread_insert(rows):
    inserted_rows=0
    try:
        connection = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="root",
            db='airbnb'
        )
        cursor = connection.cursor()
        sql_insert_query = "INSERT INTO "+constants.CALENDARS+" (listing_id, scraping_date, date, availability) VALUES (%s,%s,%s,%s)"
        inserted_rows = cursor.executemany(sql_insert_query,rows)

        connection.commit()    
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()        
        return inserted_rows

def insert_rows(scraping_date):
    thread_conn = sqlite3.connect('airbnb.db')
    thread_c = thread_conn.cursor()

    res = []
    print("Récupération des données de SQLite")
    sql = "select listing_id,'"+scraping_date+"', date, available from `" + scraping_date + "` where listing_id in common_ids and date like '"+str(constants.YEAR)+"-%'"        
    thread_c.execute(sql)
    res = thread_c.fetchall()
    print(len(res),"lignes à insérer")

    print("Temps écoulé :", time.time()-start,"secondes")

    print("Insertion dans la bdd MySQL")
    n=1000
    fragmented_res = [res[x:x+n] for x in range(0, len(res), n)]
    with ThreadPoolExecutor(max_workers = 130) as executor:
        results = list(tqdm(executor.map(thread_insert, fragmented_res),total=len(fragmented_res)))

    inserted_rows = 0
    for r in results:
       inserted_rows+=r
    print(scraping_date,"-",inserted_rows,"lignes insérées")

    thread_c.close()
    

def get_items_list(sql_request):
    c = conn.cursor()
    c.execute(sql_request)
    return [x[0] for x in c.fetchall()]

tab_date = get_items_list(
    "select distinct scraping_date from "+constants.SCRAPING_DATES+" order by scraping_date")

for scraping_date in tab_date:
    print(scraping_date)
    insert_rows(scraping_date)
    print("Temps écoulé :", time.time()-start,"secondes")
    print()