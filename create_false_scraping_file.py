import constants
import pymysql.cursors
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

connection = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="root",
            db='airbnb'
        )
cursor = connection.cursor()   

def get_items_list(sql_request):
    cursor.execute(sql_request)
    return [x[0] for x in cursor.fetchall()]

# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.CALENDARS)


def create_false_scraping_file(year,month):
    scraping_date = str(year)+'-'+(str(month-1) if month>9 else '0'+str(month-1))
    new_scraping_date = str(year)+'-'+(str(month) if month>9 else '0'+str(month))

    def thread_insert(id):
        try:
            connection = pymysql.connect(
                    host="127.0.0.1",
                    port=3306,
                    user="root",
                    password="root",
                    db='airbnb'
                )
            cursor = connection.cursor()    

            sql_select = "select listing_id,'"+new_scraping_date+"', date,availability from "+constants.CALENDARS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(scraping_date)+"' order by date"
            cursor.execute(sql_select)
            rows_to_add = cursor.fetchall()

            #On insère les lignes dans la bdd
            sql_insert = "insert into "+constants.CALENDARS+" (listing_id,scraping_date,date,availability) values (%s,%s,%s,%s)"
            inserted_rows=cursor.executemany(sql_insert,rows_to_add)
            connection.commit()    
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            connection.close()        
            return (id,inserted_rows) 


    print("Insertion des lignes pour",new_scraping_date)
    with ThreadPoolExecutor(max_workers = 130) as executor:
        results = list(tqdm(executor.map(thread_insert, listing_ids),total=len(listing_ids)))

    for r in results:
        print(scraping_date,"-",r[0],",",r[1],"lignes insérées")

#Nous allons dupliquer le fichier de scraping du mois de janvier 2018 pour créer un faux fichier de scraping du mois de février 2018
create_false_scraping_file(constants.YEAR,2)

cursor.close()
connection.close()