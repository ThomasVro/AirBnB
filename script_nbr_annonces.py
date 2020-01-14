import sqlite3
import mysql.connector
from functools import reduce
import numpy as np
import constants
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

start = time.time()

conn = sqlite3.connect('airbnb.db')
c = conn.cursor()

def querry_listing_id(querry):
    c.execute(querry)
    tab = [x[0] for x in c.fetchall()]
    return tab

#tab1 = querry_listing_id("select distinct listing_id from '2017-02'")
#tab2= querry_listing_id("select distinct listing_id from '2017-03'")
#tab3 = querry_listing_id("select distinct listing_id from '2017-04'")
#tab4 = querry_listing_id("select distinct listing_id from '2017-05'")
#tab5 = querry_listing_id("select distinct listing_id from '2017-06'")
#tab6 = querry_listing_id("select distinct listing_id from '2017-07'")
#tab7 = querry_listing_id("select distinct listing_id from '2017-08'")
#tab8 = querry_listing_id("select distinct listing_id from '2017-09'")
#tab9 = querry_listing_id("select distinct listing_id from '2017-10'")
#tab10 = querry_listing_id("select distinct listing_id from '2017-11'")
#tab11 = querry_listing_id("select distinct listing_id from '2017-12'")
#tab12 = querry_listing_id("select distinct listing_id from '2018-01'")
#tab13 = querry_listing_id("select distinct listing_id from '2018-02'")
#tab14 = querry_listing_id("select distinct listing_id from '2018-03'")
#tab15 = querry_listing_id("select distinct listing_id from '2018-04'")
#tab16 = querry_listing_id("select distinct listing_id from '2018-05'")
#tab17 = querry_listing_id("select distinct listing_id from '2018-06'")
#tab18 = querry_listing_id("select distinct listing_id from '2018-07'")
#tab19 = querry_listing_id("select distinct listing_id from '2018-08'")
#tab20 = querry_listing_id("select distinct listing_id from '2018-09'")
#tab21 = querry_listing_id("select distinct listing_id from '2018-10'")
#tab22 = querry_listing_id("select distinct listing_id from '2018-11'")
#tab23 = querry_listing_id("select distinct listing_id from '2018-12'")

#common_ids = reduce(np.intersect1d, (tab1,tab2,tab3,tab4,tab5,tab6,tab7,tab8,tab9,tab10,tab11,tab12,tab14,tab15,tab16,tab17,tab18,tab19,tab20,tab21,tab22,tab23))
#f = open("common_ids.txt","w+")
# for id in common_ids:
#    f.write(str(id) + "," +"\n")

#scraping = ['2017-02', '2017-03', '2017-04', '2017-05', '2017-06', '2017-07', '2017-08', '2017-09', '2017-10', '2017-11', '2017-12', '2018-01','2018-03', '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12']

sql = "select listing_id from common_ids"
c.execute(sql)
ids = [x[0] for x in c.fetchall()]

def thread_insert(rows):
    connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        passwd="root",
        db='airbnb'
    )
    cursor = connection.cursor()
    sql_insert_query = 'INSERT INTO calendars_2018 (listing_id, scraping_date, date, availability ) VALUES (%s,%s,%s,%s)'
    cursor.executemany(sql_insert_query,rows)

    connection.commit()    
    print(rows[0][1],"-",cursor.rowcount,"records inserted\n")

    cursor.close()
    connection.close()

def insert_rows(scraping_date):
    thread_conn = sqlite3.connect('airbnb.db')
    thread_c = thread_conn.cursor()

    res = []
    print("Récupération des données de SQLite")
    sql = "select listing_id,'"+scraping_date+"', date, available from `" + scraping_date + "` where listing_id in common_ids and date like '2018-%'"        
    thread_c.execute(sql)
    res = thread_c.fetchall()
    print(len(res),"lignes à insérer")

    print("Temps écoulé :", time.time()-start,"secondes")

    print("Insertion dans la bdd MySQL")
    n=600
    # fragmented_res = [res[i * n:(i + 1) * n] for i in range((len(res) + n - 1) // n )]
    fragmented_res = [res[x:x+n] for x in range(0, len(res), n)]
    with ThreadPoolExecutor(max_workers = 120) as executor:
        futures = executor.map(thread_insert, fragmented_res)

    for f in as_completed(futures):
        f.result()

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


# conn = sqlite3.connect('AirBnB.db')
# c = conn.cursor()

# tab_id=[18028290, 4915424, 11899722, 4723995, 6256910, 7276681, 7568471, 6606225, 5149727, 12576118]
# tab_date=['2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01','2018-03','2018-04','2018-05']
# #sql = "Select * from ? where listing_id=?"
# f = open('donnee.csv','w')
# for listing_id in tab_id:
#     for j in tab_date:
#         sql = "Select * from " + "'"+str(j)+"'" + " where listing_id=" + str(listing_id)
#         c.execute(sql)
#         res =c.fetchall()
#         for r in res:
#             f.write(str(r[0]) + "," + j + "," + r[1] + "," + r[2] + "\n")
# f.close()
