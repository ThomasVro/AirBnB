import json
import sqlite3
import constants
import pymysql.cursors
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

dict_months = {
    "janvier": "01",
    "février": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "août": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "décembre": "12"
}

values = []
id = 0
with open('booking.txt', encoding='utf-8') as json_file:
    data = json.load(json_file)
    for d in data:
        conn = sqlite3.connect('AirBnB.db')
        c = conn.cursor()
        sql = "select listing_id from "+constants.LINKS_BOOKING_SQLITE + \
            " where booking =" + "'" + str(d['id']) + "'"
        c.execute(sql)
        tab = [x[0] for x in c.fetchall()]
        if len(tab) > 0:
            listing_id = tab[0]
            for elt in d['dates']:
                if len(elt) > 20:
                    elt = elt.replace("Commentaire envoyé le ", '')
                    t = elt.split(' ')
                    if len(t[0]) == 1:
                        day = "0" + t[0]
                    else:
                        day = t[0]

                    months = dict_months[t[1]]
                    year = t[2]
                    format_date = year + "-" + months + "-" + day    

                    values.append((str(listing_id),str(format_date),"B"+str(id),"Comment from Booking.com"))
                    id+=1

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
        sql_insert_query = "INSERT INTO "+constants.BOOKING_COMMENTS+" (listing_id, date, reviewer_id,comments) VALUES (%s,%s,%s,%s)"
        inserted_rows = cursor.executemany(sql_insert_query,rows)

        connection.commit()    
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()        
        return inserted_rows

print("Insertion dans la bdd MySQL")
n=1000
fragmented_res = [values[x:x+n] for x in range(0, len(values), n)]
with ThreadPoolExecutor(max_workers = 130) as executor:
    results = list(tqdm(executor.map(thread_insert, fragmented_res),total=len(fragmented_res)))

inserted_rows = 0
for r in results:
    inserted_rows+=r
print(inserted_rows,"lignes insérées")