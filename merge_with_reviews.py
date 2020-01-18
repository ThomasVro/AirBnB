import pandas as pd
import sqlite3
import csv 

conn = sqlite3.connect('AirBnB.db')
c = conn.cursor()
query = "select * from data_2018 where scraping_date = '2017-02'"

frame_data = pd.read_sql_query(query, conn)

query = "select * from reviews_2018_11"
frame_2017_06 = pd.read_sql_query(query, conn)

merge = pd.merge(frame_data, frame_2017_06, how = 'left', on = ['listing_id', 'date'])
merge.to_csv('merge_comments.csv', encoding = 'utf-8', index = False)