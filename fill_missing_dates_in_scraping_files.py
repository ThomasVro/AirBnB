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


year = constants.YEAR

# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.LISTINGS+" order by listing_id")

# Liste des dates de scraping
scraping_dates = get_items_list(
    "select distinct scraping_date from "+constants.LISTINGS)

#Les fichiers de scraping commencent à partir de la date à laquelle ils ont été récupéré.
#Nous récupérons les données du fichier de scraping précédent qui commence à la bonne date, en supposant que les données n'ont pas changé
mandatory_starting_date = datetime.datetime(year,1,1)#On veut que tous les fichiers de scraping commencent à cette date

for id in listing_ids:
    print(id)
    for scraping_date in scraping_dates:
        scraping = get_items_fetchall("select scraping_date,availability,min(date) from "+constants.LISTINGS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(scraping_date)+"'")[0]
        starting_date = datetime.datetime.strptime(scraping[2],"%Y-%m-%d")
        rows_to_add = []#Va contenir les données manquantes que l'on va insérer en bdd
        if starting_date != mandatory_starting_date:            
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
            previous_scraping = get_items_fetchall("select * from "+constants.LISTINGS+" where listing_id = '"+str(id)+"' and scraping_date ='"+str(previous_scraping_date)+"' order by date")
            
            for s in previous_scraping:
                date = datetime.datetime.strptime(s[2],"%Y-%m-%d")
                if date < starting_date:
                    value = (s[0],scraping[0],s[2],s[3])
                    rows_to_add.append(value)

            #On insère les lignes dans la bdd
            c=conn.cursor()
            for row in rows_to_add:
                sql_insert = "insert into "+constants.LISTINGS+" (listing_id,scraping_date,date,availability) values "+str(row)
                c.execute(sql_insert)

conn.commit()
conn.close()