import sqlite3
import json
import datetime
import calendar
year = 2018
month = 5
num_days = calendar.monthrange(year, month)[1]
days = [datetime.date(year, month, day).strftime("%Y-%m-%d")
        for day in range(1, num_days+1)]

# liste des dates de scraping
conn = sqlite3.connect('listing_and_reviews_10annonces.db')
c = conn.cursor()
sql = "select distinct scraping_date from listing_and_reviews"
c.execute(sql)
tab_date = [x[0] for x in c.fetchall()]

# liste des annonces
c = conn.cursor()
sql = "select distinct listing_id from listing_and_reviews order by listing_id"
c.execute(sql)
listing_ids = [x[0] for x in c.fetchall()]

interpretation = {}
for listing_id in listing_ids:  # pour chaque annonce
    print(listing_id)

    i = 0
    j = 1
    interpretation[listing_id] = {}
    temp_month1 = {}  # Pour stocker le mois 2 et ne pas avoir à le recharger via SQL en mois 1 lors de la prochaine comparaison

    # Liste des véritables périodes indisponibles
    synthese_periodes_indisponibles = []

    while j < len(tab_date):
        # on construit une paire de deux dates de scraping
        paire = [tab_date[i], tab_date[j]]

        # On remplit chaque tableau avec les données des scrapings correspondant
        c = conn.cursor()
        sql = ""
        if temp_month1 == {}:
            sql = "select * from listing_and_reviews where listing_id='" + \
                str(listing_id)+"' and scraping_date='" + \
                paire[0]+"' order by scraping_date, date"
            c.execute(sql)
            month1 = c.fetchall()
        else:
            month1 = temp_month1

        sql = "select * from listing_and_reviews where listing_id='" + \
            str(listing_id)+"' and scraping_date='" + \
            paire[1]+"' order by scraping_date, date"
        c.execute(sql)
        month2 = c.fetchall()
        temp_month1 = month2

        # On compte le nombre de jours réservés pour chaque fichier
        month1_reservations = 0
        month2_reservations = 0

        month1_dispos = []
        month2_dispos = []

        month1_reservations_liste = []
        month2_reservations_liste = []

        month1_reservationStarted = False
        month2_reservationStarted = False

        month1_reservationEnded = False
        month2_reservationEnded = False

        month1_reservationStarted_date = ""
        month1_reservationEnded_date = ""

        month2_reservationStarted_date = ""
        month2_reservationEnded_date = ""

        count_reservations = 0

        for jour in month1:
            if jour[2] in days:
                dispo = jour[3]
                month1_dispos.append(dispo)
                if dispo == 'f':
                    if count_reservations == 0:
                        month1_reservationStarted = True
                        month1_reservationEnded = False
                        month1_reservationStarted_date = jour[2]
                    if month1_reservationStarted:
                        count_reservations += 1
                    if str(jour[2]) == days[-1] and month1_reservationStarted:
                        count_reservations = 0
                        month1_reservations_liste.append(
                            (month1_reservationStarted_date, days[-1]))
                    month1_reservations += 1
                if dispo == 't' and month1_reservationStarted:
                    month1_reservationStarted = False
                    month1_reservationEnded = True
                    month1_reservationEnded_date = month1[month1.index(
                        jour)-1][2]
                if month1_reservationEnded and (month1_reservationStarted_date, month1_reservationEnded_date) not in month1_reservations_liste:
                    count_reservations = 0
                    month1_reservations_liste.append(
                        (month1_reservationStarted_date, month1_reservationEnded_date))

        count_reservations = 0

        for jour in month2:
            if jour[2] in days:
                dispo = jour[3]
                month2_dispos.append(dispo)
                if dispo == 'f':
                    if count_reservations == 0:
                        month2_reservationStarted = True
                        month2_reservationEnded = False
                        month2_reservationStarted_date = jour[2]
                    if month2_reservationStarted:
                        count_reservations += 1
                    if str(jour[2]) == days[-1] and month2_reservationStarted:
                        count_reservations = 0
                        month2_reservations_liste.append(
                            (month2_reservationStarted_date, days[-1]))
                    month2_reservations += 1
                if dispo == 't' and month2_reservationStarted:
                    month2_reservationStarted = False
                    month2_reservationEnded = True
                    month2_reservationEnded_date = month2[month2.index(
                        jour)-1][2]
                if month2_reservationEnded and (month2_reservationStarted_date, month2_reservationEnded_date) not in month2_reservations_liste:
                    count_reservations = 0
                    month2_reservations_liste.append(
                        (month2_reservationStarted_date, month2_reservationEnded_date))

        # S'il n'y a pas de changements
        interpretation[listing_id][str(paire)] = {}
        if month1_reservations == month2_reservations:
            interpretation[listing_id][str(paire)]["Total"] = str(
                month1_reservations)+" jours indisponibles"
        # Si on a des des jours indispo en plus
        if month1_reservations < month2_reservations:
            interpretation[listing_id][str(paire)]["Total"] = "+ "+str(
                month2_reservations-month1_reservations)+" jours indisponibles"
        # Si on a des jours indispo en moins
        if month1_reservations > month2_reservations:
            interpretation[listing_id][str(paire)]["Total"] = "- "+str(
                month1_reservations-month2_reservations)+" jours indisponibles"

        # Détails changement
        annulations = 0
        reservations = 0
        for dispo, dispo2 in zip(month1_dispos, month2_dispos):
            if dispo == 't' and dispo2 == 'f':
                reservations += 1
            if dispo == 'f' and dispo2 == 't':
                annulations += 1
        interpretation[listing_id][str(paire)]["Reservations"] = {
            "nombre total de jours": reservations,
            str(paire[0]): month1_reservations_liste,
            str(paire[1]): month2_reservations_liste,
        }
        interpretation[listing_id][str(paire)]["Annulations"] = annulations

        # print(paire)
        # print(month1_dispos)
        # print(month1_reservations_liste)
        # print(month2_dispos)
        # print(month2_reservations_liste)

        # Périodes réellement indispo
        month1_reservations_liste_date = []
        month1_jours_indispo = 0
        month2_reservations_liste_date = []
        month2_jours_indispo = 0
        for periode in month1_reservations_liste:
            periode_date = (datetime.datetime.strptime(periode[0], "%Y-%m-%d").date(), datetime.datetime.strptime(periode[1], "%Y-%m-%d").date())
            month1_jours_indispo+=(periode_date[1]-periode_date[0]).days+1
            month1_reservations_liste_date.append(periode_date)
        for periode in month2_reservations_liste:
            periode_date = (datetime.datetime.strptime(periode[0], "%Y-%m-%d").date(), datetime.datetime.strptime(periode[1], "%Y-%m-%d").date())
            month2_jours_indispo+=(periode_date[1]-periode_date[0]).days+1
            month2_reservations_liste_date.append(periode_date)
        
        print(paire)
        print(month1_reservations_liste_date)
        print()
        print(month2_reservations_liste_date)
        print()

        # if month1_jours_indispo == month2_jours_indispo:
        #     for periode in month2_reservations_liste_datetime:
        #         synthese_periodes_indisponibles.append(periode)
        # #Si tout était indispo et que le calendrier a été ouvert puis certaines périodes indispo
        # if month1_jours_indispo==31 and month2_jours_indispo<31:
        #     synthese_periodes_indisponibles = []
        #     for periode in month2_reservations_liste_datetime:
        #         synthese_periodes_indisponibles.append(periode)
        
        i = j
        j += 1


    # On trie par date de début de périodes indispo
    synthese_periodes_indisponibles = sorted(
        synthese_periodes_indisponibles, key=lambda x: x[0])
    for periode in synthese_periodes_indisponibles:
        print(periode)

    # Nettoyage des périodes
    # if len(synthese_periodes_indisponibles)>1:
    #     synthese_periodes_indisponibles_cleaned = synthese_periodes_indisponibles.copy()
    #     temp = []
    #     for cur, nxt in zip (synthese_periodes_indisponibles, synthese_periodes_indisponibles[1:]):
    #         if cur[0]==nxt[0] and cur[1]<nxt[1]:
    #             temp.append(cur)
    #             periode_englobante = (nxt[0],nxt[1])
    #             break
    #     print("Periode englobante")
    #     print(periode_englobante)
    #     for periode in synthese_periodes_indisponibles:
    #         if periode[0]>periode_englobante[0]:
    #             if periode[1]<periode_englobante[1] or periode[1]==periode_englobante[1]:
    #                 temp.append(periode)
    #     print(temp)
    # for periode in synthese_periodes_indisponibles_cleaned:
    #     print(periode)
    # print()
    # for cur, nxt in zip (synthese_periodes_indisponibles, synthese_periodes_indisponibles[1:]):
    #     if cur[0]==nxt[0] and cur[1]<nxt[1]:
    #         i = synthese_periodes_indisponibles_cleaned.index(nxt)
    #         new_periode = (cur[1]+datetime.timedelta(days=1),nxt[1])
    #         synthese_periodes_indisponibles_cleaned.append(new_periode)
    #         # synthese_periodes_indisponibles_cleaned.remove(synthese_periodes_indisponibles_cleaned[i])
    # synthese_periodes_indisponibles_cleaned = sorted(synthese_periodes_indisponibles_cleaned,key=lambda x:x[0])

    #On retire les périodes qui sont plus larges que d'autres
    #Ex : Du 2018-05-01 au 2018-05-31 est plus large que 2018-05-09 au 2018-05-31
    # donc on retire la première période
    # for cur, nxt in zip (synthese_periodes_indisponibles, synthese_periodes_indisponibles[1:]):
    #     if (cur[0]<nxt[0] and cur[1]==nxt[1]) or (cur[0]==nxt[0] and cur[1]>nxt[1]):
    #         i = synthese_periodes_indisponibles.index(cur)
    #         synthese_periodes_indisponibles_cleaned.remove(synthese_periodes_indisponibles[i])
    # print()
    # for periode in synthese_periodes_indisponibles_cleaned:
    #     print(periode)

    # #On décompose les périodes englobants une période précédente indispo
    # #Ex : Du 2018-05-15 au 2018-05-17 est comprise dans 2018-05-15 au 2018-05-31
    # # donc on remplace la 2e période par (2018-05-18 au 2018-05-31)
    # for cur, nxt in zip (synthese_periodes_indisponibles, synthese_periodes_indisponibles[1:]):
    #     if cur[0]==nxt[0] and cur[1]<nxt[1]:
    #         #On retire nxt
    #         i = synthese_periodes_indisponibles_cleaned.index(nxt)
    #         new_periode = (cur[1]+datetime.timedelta(days=1),nxt[1])
    #         synthese_periodes_indisponibles_cleaned[i] = new_periode
    # print()
    # for periode in synthese_periodes_indisponibles_cleaned:
    #     print(periode)

    # #Dernier cleaning
    # synthese_periodes_indisponibles = synthese_periodes_indisponibles_cleaned.copy()
    # for cur, nxt in zip (synthese_periodes_indisponibles, synthese_periodes_indisponibles[1:]):
    #     if cur[0]<nxt[0] and cur[1]>nxt[1]:
    #         #On retire nxt
    #         i = synthese_periodes_indisponibles_cleaned.index(cur)
    #         new_periode = (cur[0],nxt[0]-datetime.timedelta(days=1))
    #         synthese_periodes_indisponibles_cleaned[i] = new_periode
    # print()
    # for periode in synthese_periodes_indisponibles_cleaned:
    #     print(periode)

with open('results.json', 'w') as outfile:
    json.dump(interpretation, outfile)
print("Fait.")
