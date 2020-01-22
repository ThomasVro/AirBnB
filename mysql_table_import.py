import sqlite3
import pymysql.cursors
from concurrent.futures import ThreadPoolExecutor
import constants
from tqdm import tqdm

sqlite3_connection = sqlite3.connect('airbnb.db')
sqlite3_cursor = sqlite3_connection.cursor()

#On récupère les commentaires de 2018
sql_select = "select listing_id,date,reviewer_id,reviewer_name,comments from '"+constants.REVIEWS_SQLITE+"' where date like '2018-%' and listing_id in "+constants.COMMON_IDS
sqlite3_cursor.execute(sql_select)
reviews_2018 = sqlite3_cursor.fetchall()

sqlite3_cursor.close()
sqlite3_connection.close()

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
        sql_insert_query = "INSERT INTO "+constants.REVIEWS_MYSQL+" (listing_id,date,reviewer_id,reviewer_name,comments) VALUES (%s,%s,%s,%s,%s)"
        inserted_rows = cursor.executemany(sql_insert_query,rows)
        connection.commit()    
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()        
        return inserted_rows

print("Insertion de",len(reviews_2018),"dans la base de données")
n=1000
fragmented_res = [reviews_2018[x:x+n] for x in range(0, len(reviews_2018), n)]
with ThreadPoolExecutor(max_workers = 130) as executor:
    results = list(tqdm(executor.map(thread_insert, fragmented_res),total=len(fragmented_res)))

inserted_rows = 0
for r in results:
    inserted_rows+=r
print(inserted_rows,"lignes insérées")