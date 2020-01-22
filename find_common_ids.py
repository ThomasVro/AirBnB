import sqlite3
import numpy as np
from functools import reduce
import constants
import csv

conn = sqlite3.connect('airbnb.db')
c = conn.cursor()

def querry_listing_id(tab):
    c.execute("select distinct listing_id from `"+tab+"`")
    print("Données récupérées")
    tab = [x[0] for x in c.fetchall()]
    return tab

def get_items_list(sql_request):
    c.execute(sql_request)
    return [x[0] for x in c.fetchall()]

scraping_dates = get_items_list("select scraping_date from "+constants.SCRAPING_DATES)
tabs=[]
for scraping_date in scraping_dates:
    print(scraping_date)
    tabs.append(querry_listing_id(scraping_date))

print("Récupération des listing_ids communs")
common_ids = reduce(np.intersect1d, tabs)
print(common_ids)
print(len(common_ids))

rows=['listing_id']
rows.extend(common_ids.tolist())
rows = [[x] for x in rows]

print("Création du CSV")
with open('common_ids.csv','w',newline='') as f:
    writer = csv.writer(f)
    writer.writerows(rows)