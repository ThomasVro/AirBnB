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
    interpretation[listing_id]["Details"] = {}
    temp_month1 = {}  # Pour stocker le mois 2 et ne pas avoir à le recharger via SQL en mois 1 lors de la prochaine comparaison

    # Liste des véritables périodes indisponibles
    synthese_periodes_indisponibles = [0]*num_days

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
        # interpretation[listing_id][str(paire)]["Reservations"] = {
        #     "nombre total de jours": reservations,
        #     str(paire[0]): month1_reservations_liste,
        #     str(paire[1]): month2_reservations_liste,
        # }
        interpretation[listing_id][str(paire)]["Reservations"] = reservations
        interpretation[listing_id][str(paire)]["Annulations"] = annulations

        interpretation[listing_id]["Details"][str(
            paire[0])] = month1_reservations_liste
        interpretation[listing_id]["Details"][str(
            paire[1])] = month2_reservations_liste

        i = j
        j += 1

    compteur_general = 1
    for scraping in tab_date:
        month_reservations = interpretation[listing_id]["Details"][str(
            scraping)]
        month_reservations_dates = []
        for periode in month_reservations:
            periode_date = (datetime.datetime.strptime(
                periode[0], "%Y-%m-%d").date(), datetime.datetime.strptime(periode[1], "%Y-%m-%d").date())
            month_reservations_dates.append(periode_date)
        month_reservations_dates = sorted(
            month_reservations_dates, key=lambda x: x[0].day)

        temp = [0]*num_days
        compteur = 0
        for periode in month_reservations_dates:
            compteur += 1
            for i in range(periode[0].day, periode[1].day+1):
                temp[i-1] = compteur

        for date in range(num_days):
            # Réservation
            if synthese_periodes_indisponibles[date] == 0 and temp[date] != 0:
                synthese_periodes_indisponibles[date] = compteur_general
            # Annulation
            if synthese_periodes_indisponibles[date] != 0 and temp[date] == 0:
                synthese_periodes_indisponibles[date] = 0
            if date != num_days-1:
                if temp[date] != 0 and temp[date+1] != temp[date]:
                    compteur_general += 1

    compteur_day = 0
    liste_periodes = []
    # Initialisation des compteurs (car on ne peut pas comparer à un jour précédent)
    if synthese_periodes_indisponibles[0] == 0:
        compteur_periodes = 0
        compteur_day = 0
    else:
        date_debut = datetime.date(year, month, 1)
        compteur_periodes = 1
        compteur_day = 1
    for i in range(1, num_days):
        if synthese_periodes_indisponibles[i] != synthese_periodes_indisponibles[i-1]:
            if synthese_periodes_indisponibles[i] != 0:
                compteur_periodes += 1
            if synthese_periodes_indisponibles[i] != 0 and synthese_periodes_indisponibles[i-1] != 0:
                date_fin = datetime.date(year, month, i)
                liste_periodes.append((date_debut, date_fin))
                date_debut = datetime.date(year, month, i+1)
            if synthese_periodes_indisponibles[i] == 0 and synthese_periodes_indisponibles[i-1] != 0:
                date_fin = datetime.date(year, month, i)
                liste_periodes.append((date_debut, date_fin))
            if synthese_periodes_indisponibles[i-1] == 0 and synthese_periodes_indisponibles[i] != 0:
                date_debut = datetime.date(year, month, i+1)
        if synthese_periodes_indisponibles[i] != 0:
            compteur_day += 1
    if synthese_periodes_indisponibles[-1] != 0:
        date_fin = datetime.date(year, month, num_days)
        liste_periodes.append((date_debut, date_fin))

    liste_periodes_string = []
    for periode in liste_periodes:
        liste_periodes_string.append((periode[0].strftime(
            "%Y-%m-%d"), periode[1].strftime("%Y-%m-%d")))
    if liste_periodes != [(datetime.date(year, month, 1), datetime.date(year, month, num_days))]:
        interpretation[listing_id]["Synthese"] = {
            "Jours indisponibles": compteur_day,
            "Nombre de reservations distinctes": compteur_periodes,
            "Liste": liste_periodes_string
        }
    # Si le propriétaire a fermé tout le mois
    else:
        if compteur_day == num_days:
            interpretation[listing_id]["Synthese"] = "Le proprietaire a ferme tout le mois."
        if compteur_day == 0:
            interpretation[listing_id]["Synthese"] = "Le proprietaire n'a pas eu de reservations."

    # Liaison avec les commentaires de l'annonce
    liaisons = {}
    if liste_periodes != [(datetime.date(year, month, 1), datetime.date(year, month, num_days))] and liste_periodes != []:
        sql = "select * from listing_and_reviews where listing_id='" + \
            str(listing_id)+"' and scraping_date = '" + \
            str(tab_date[0])+"' and id not null order by date"
        c.execute(sql)
        commentaires = c.fetchall()
        # ETAPE 1 : commentaires dans les 15 jours après la réservation
        for periode in liste_periodes:
            liaisons[str(periode)] = {}
            for commentaire in commentaires:
                date_du_commentaire = datetime.datetime.strptime(
                    commentaire[2], "%Y-%m-%d").date()
                if periode[1] < date_du_commentaire and periode[1] > date_du_commentaire-datetime.timedelta(days=14):
                    liaisons[str(periode)][commentaire[4]] = {
                        "date": commentaire[2],
                        "reviewer_id": commentaire[5],
                        "reviewer_name": commentaire[6],
                        "contenu": commentaire[7]
                    }
        # ETAPE 2 : compter le nombre de réservations qui se sont terminées 15 jours max avant chaque commentaire
        # Pour déterminer le pourcentage de chance qu'un commentaire corresponde à une résa.
        for commentaire in commentaires:
            nb_resa = 0
            date_du_commentaire = datetime.datetime.strptime(
                commentaire[2], "%Y-%m-%d").date()
            for periode in liste_periodes:
                if periode[1] < date_du_commentaire and periode[1] > date_du_commentaire-datetime.timedelta(days=14):
                    nb_resa += 1
            for periode in liaisons:
                if commentaire[4] in liaisons[str(periode)]:
                    pourcentage = (1/nb_resa)*100
                    liaisons[str(periode)][commentaire[4]
                                           ]["pourcentage"] = (1/nb_resa)*100
        # ETAPE 3 : retirer les commentaires de moins de 100% pour une réservation qui a déjà un commentaire à 100%
        continuer = True
        while continuer:
            continuer = False
            for periode in liste_periodes:
                found = False
                id_a_modif = []
                # Pour chaque période, on cherche si un des commentaires est à 100%
                for commentaire in liaisons[str(periode)]:
                    if liaisons[str(periode)][commentaire]["pourcentage"] == 100:
                        id_Cent = commentaire
                        found = True
                    else:
                        id_a_modif.append(commentaire)
                # Si c'est le cas
                if found and id_a_modif != []:
                    temp = {}
                    # On ne laisse que le commentaire à 100% pour cette période
                    for commentaire in liaisons[str(periode)]:
                        if commentaire == id_Cent:
                            temp[commentaire] = liaisons[str(
                                periode)][commentaire]
                    liaisons[str(periode)] = temp
                    # On met à jour les pourcentages des commentaires liés à d'autres périodes
                    for id in id_a_modif:
                        for periode in liste_periodes:
                            if id in liaisons[str(periode)]:
                                ancien_nb_resa = int(
                                    100/liaisons[str(periode)][id]["pourcentage"])
                                if ancien_nb_resa > 0:
                                    nouveau_pourcentage = (
                                        1/(ancien_nb_resa-1))*100
                                    liaisons[str(
                                        periode)][id]["pourcentage"] = nouveau_pourcentage
                                    if not continuer and nouveau_pourcentage == 100:
                                        continuer = True
        # ETAPE 4 : si on a déjà deux commentaires à 50%, on retire les autres avec une proba plus faible
        continuer = True
        while continuer:
            continuer = False
            for periode in liste_periodes:
                found = False
                id_a_modif = []
                id_50_1 = None
                id_50_2 = None
                # Pour chaque période, on cherche si un des commentaires est à 50%
                for commentaire in liaisons[str(periode)]:
                    if liaisons[str(periode)][commentaire]["pourcentage"] == 50:
                        if id_50_1 == None:
                            id_50_1 = commentaire
                        elif id_50_2 == None:
                            id_50_2 = commentaire
                        found = True
                    else:
                        id_a_modif.append(commentaire)
                # Si c'est le cas
                if found and id_a_modif != []:
                    temp = {}
                    # On ne laisse que les commentaire à 50% pour cette période
                    for commentaire in liaisons[str(periode)]:
                        if commentaire == id_50_1 or commentaire == id_50_2:
                            temp[commentaire] = liaisons[str(
                                periode)][commentaire]
                    liaisons[str(periode)] = temp
                    # On met à jour les pourcentages des commentaires liés à d'autres périodes
                    for id in id_a_modif:
                        for periode in liste_periodes:
                            if id in liaisons[str(periode)]:
                                ancien_nb_resa = int(
                                    100/liaisons[str(periode)][id]["pourcentage"])
                                if ancien_nb_resa > 0:
                                    nouveau_pourcentage = (
                                        1/(ancien_nb_resa-1))*100
                                    liaisons[str(
                                        periode)][id]["pourcentage"] = nouveau_pourcentage
                                    if not continuer and nouveau_pourcentage == 50:
                                        continuer = True
        #ETAPE INTERMEDIAIRE : Nettoyage commentaire à 50% tout seul
        id_a_clean = []
        for periode in liste_periodes:
            # Pour chaque période, on cherche si on a un commentaire unique à 50%
            for commentaire in liaisons[str(periode)]:
                if liaisons[str(periode)][commentaire]["pourcentage"] == 50 and len(liaisons[str(periode)]) == 1:
                    id_a_clean.append(commentaire)
        # On cherche si un des commentaires trouvés apparaît une seule fois
        for id in id_a_clean:
            count = 0
            for periode in liste_periodes:
                for commentaire in liaisons[str(periode)]:
                    if commentaire == id:
                        count += 1
            if count == 1:
                for periode in liste_periodes:
                    for commentaire in liaisons[str(periode)]:
                        if commentaire == id:
                            liaisons[str(periode)
                                     ][commentaire]["pourcentage"] = 100.0

        # ETAPE 5 : Entre deux commentaires à 50% pour une période, on garde le commentaire le plus proche en date donc le premier
        liste_id_a_garder = []
        # truc = False
        # for periode in liste_periodes:
        #     temp = {}
        #     for commentaire in liaisons[str(periode)]:
        #         if liaisons[str(periode)][commentaire]["pourcentage"] == 50:
        #             if liste_id_a_garder == []:
        #                 liste_id_a_garder.append(commentaire)
        #                 temp[commentaire] = liaisons[str(periode)][commentaire]
        #                 truc = True
        #             if liste_id_a_garder != []:
        #                 if truc:
        #                     if commentaire not in liste_id_a_garder:
        #                         liaisons[str(periode)] = temp[commentaire]
        #                         truc = False
        #                 if not truc:
        #                     if commentaire in liste_id_a_garder:



                        
        # Count nombre de commentaires à 100%
        count_100percent = 0
        for periode in liste_periodes:
            # Pour chaque période, on cherche si un des commentaires est à 100%
            for commentaire in liaisons[str(periode)]:
                if liaisons[str(periode)][commentaire]["pourcentage"] == 100:
                    count_100percent += 1
        # On l'ajoute à results.json dans la partie Synthèse de l'annonce
        interpretation[listing_id]["Synthese"]["Nombre de periodes validees a 100%"] = count_100percent

    with open('liaisons_'+str(listing_id)+'.json', 'w') as outfile:
        json.dump(liaisons, outfile)

with open('results.json', 'w') as outfile:
    json.dump(interpretation, outfile)
print("Fait.")
