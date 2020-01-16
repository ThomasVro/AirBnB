import constants
import datetime
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

def get_items_fetchall(sql_request):
    cursor.execute(sql_request)
    return cursor.fetchall()

year = constants.YEAR

print("Récupération des listing_id et dates de scraping")
# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.CALENDARS)

# Liste des dates de scraping
scraping_dates = get_items_list(
    "select distinct scraping_date from "+constants.CALENDARS)

#Les fichiers de scraping commencent à partir de la date à laquelle ils ont été récupéré.
#Nous récupérons les données du fichier de scraping précédent qui commence à la bonne date, en supposant que les données n'ont pas changé
mandatory_starting_date = datetime.datetime(year,1,1)#On veut que tous les fichiers de scraping commencent à cette date

def thread_insert(row):
    inserted_rows=0
    try:
        thread_connection = pymysql.connect(
                host="127.0.0.1",
                port=3306,
                user="root",
                password="root",
                db='airbnb'
            )
        thread_cursor = connection.cursor()
        #On insère les lignes dans la bdd
        sql_insert = "insert into "+constants.CALENDARS+" (listing_id,scraping_date,date,availability) values (%s,%s,%s,%s)"
        print(row)
        # inserted_row=thread_cursor.execute(sql_insert,row)
        # thread_connection.commit()    
    except Exception as e:
        print(e)
    finally:
        thread_cursor.close()
        thread_connection.close()        
        return inserted_rows

for id in listing_ids:
    for scraping_date in scraping_dates:
        scraping = get_items_fetchall("select scraping_date,availability,min(date) from "+constants.CALENDARS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(scraping_date)+"'")[0]
        print(id,scraping)
        starting_date = datetime.datetime.strptime(scraping[2],"%Y-%m-%d")
        rows_to_add = []#Va contenir les données manquantes que l'on va insérer en bdd
        if starting_date != mandatory_starting_date: 
            print("oui")           
            #Si la date de début du fichier de scraping est après la date obligatoire
            #On doit combler avec les data du dernier fichier de scraping qui contient les données manquantes
            #Ex : pour 2018-01, le fichier commence à 2018-01-16 donc on va prendre les données de 2017-12

            #On récupère le fichier de scraping précédent
            if int(scraping[0].split('-')[1]) > 1:
                previous_scraping_year = scraping[0].split('-')[0]
                previous_scraping_month = str(int(scraping[0].split('-')[1]) -1) if int(scraping[0].split('-')[1]) -1 > 9 else '0'+str(int(scraping[0].split('-')[1]) -1)
            else:
                previous_scraping_year = int(scraping[0].split('-')[0]) -1
                previous_scraping_month = 12
            previous_scraping_date = str(previous_scraping_year)+'-'+str(previous_scraping_month)

            #On va compléter le fichier de scraping avec les données du fichier du mois d'avant
            previous_scraping = get_items_fetchall("select listing_id,scraping_date,date,availability from "+constants.CALENDARS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(previous_scraping_date)+"' order by date")
            
            for s in previous_scraping:
                date = datetime.datetime.strptime(s[2],"%Y-%m-%d")
                if date < starting_date:
                    listing_id = s[0]
                    s_date = scraping[0]
                    d = s[2]
                    availability = s[3]
                    exists = get_items_fetchall("select * from "+constants.CALENDARS+" where listing_id = "+str(listing_id)+" and scraping_date ='"+str(s_date)+"' and date = '"+str(d)+"' and availability='"+str(availability)+"' order by date")
                    if not exists:
                        rows_to_add.append((listing_id,s_date,d,availability))
                    else:
                        print("yes")


            #On insère les lignes dans la bdd
            # with ThreadPoolExecutor(max_workers = 130) as executor:
            #     results = list(tqdm(executor.map(thread_insert, rows_to_add),total=len(rows_to_add)))

            # sql_insert = "insert into "+constants.CALENDARS+" (listing_id,scraping_date,date,availability) values (%s,%s,%s,%s)"
            # inserted_rows=cursor.executemany(sql_insert,rows_to_add)
            # connection.commit()   

            # print(scraping_date,"-",id,inserted_rows,"lignes insérées")    

cursor.close()
connection.close()