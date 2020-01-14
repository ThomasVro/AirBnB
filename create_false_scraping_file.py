import constants
import sqlite3
import mysql.connector
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

conn = sqlite3.connect('airbnb.db')



def get_items_list(sql_request):
    c = conn.cursor()
    c.execute(sql_request)
    return [x[0] for x in c.fetchall()]


def get_items_fetchall(sql_request):
    c = conn.cursor()
    c.execute(sql_request)
    return c.fetchall()

#Nous allons dupliquer le fichier de scraping du mois de janvier 2018 pour créer un faux fichier de scraping du mois de février 2018
year = constants.YEAR
month = 2

scraping_date = str(year)+'-'+(str(month-1) if month>9 else '0'+str(month-1))
new_scraping_date = str(year)+'-'+(str(month) if month>9 else '0'+str(month))

# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.COMMON_IDS+" order by listing_id")

# for id in listing_ids:
#     print(id)
#     scraping_date = str(year)+'-'+(str(month-1) if month>9 else '0'+str(month-1))
#     new_scraping_date = str(year)+'-'+(str(month) if month>9 else '0'+str(month))
#     sql_select = "select * from "+constants.CALENDAR+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(scraping_date)+"' order by date"
#     c_mysql.execute(sql_select)
#     scraping = c_mysql.fetchall()
#     rows_to_add = []
#     for s in scraping:
#         value = (s[0],new_scraping_date,s[2],s[3])
#         rows_to_add.append(value)

#     #On insère les lignes dans la bdd
#     sql_insert = "insert into "+constants.CALENDAR+" (listing_id,scraping_date,date,availability) values (%s,%s,%s,%s)"
#     c_mysql.executemany(sql_insert,rows_to_add)
#     mydb.commit()    
#     print(c_mysql.rowcount,"records inserted")
#     c_mysql.close()

def thread_insert(id):
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        passwd="root",
        db='airbnb'
    )
    c_mysql = mydb.cursor()    

    sql_select = "select listing_id,'"+new_scraping_date+"', date,availability from "+constants.CALENDARS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(scraping_date)+"' order by date"
    c_mysql.execute(sql_select)
    rows_to_add = c_mysql.fetchall()
    print(id,len(rows_to_add),"rows to insert")

    #On insère les lignes dans la bdd
    # sql_insert = "insert into "+constants.CALENDARS+" (listing_id,scraping_date,date,availability) values (%s,%s,%s,%s)"
    # c_mysql.executemany(sql_insert,rows_to_add)
    # mydb.commit()    
    # print(c_mysql.rowcount,"records inserted")
    # c_mysql.close()

with ThreadPoolExecutor(max_workers = 150) as executor:
        futures = executor.map(thread_insert, listing_ids)

conn.close()