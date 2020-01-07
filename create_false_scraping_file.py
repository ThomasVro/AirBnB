import constants
import sqlite3
import datetime

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

# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.LISTINGS+" order by listing_id")

for id in listing_ids:
    print(id)
    scraping_date = str(year)+'-'+(str(month-1) if month>9 else '0'+str(month-1))
    new_scraping_date = str(year)+'-'+(str(month) if month>9 else '0'+str(month))
    scraping = get_items_fetchall("select * from "+constants.LISTINGS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(scraping_date)+"' order by date")
    rows_to_add = []
    for s in scraping:
        value = (s[0],new_scraping_date,s[2],s[3])
        rows_to_add.append(value)

    #On insère les lignes dans la bdd
    c=conn.cursor()
    for row in rows_to_add:
        sql_insert = "insert into "+constants.LISTINGS+" (listing_id,scraping_date,date,availability) values "+str(row)
        c.execute(sql_insert)

conn.commit()
conn.close()