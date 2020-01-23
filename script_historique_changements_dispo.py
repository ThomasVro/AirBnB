# -*-coding:UTF-8 -*
import constants
import sqlite3
import json
import datetime
import calendar
import pymysql.cursors
from concurrent.futures import ThreadPoolExecutor
import constants
from tqdm import tqdm
from threading import Lock


def get_items_list(sql_request):
    connection = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root",
        db='airbnb'
    )
    cursor = connection.cursor()
    cursor.execute(sql_request)
    cursor.close()
    connection.close()
    return [x[0] for x in cursor.fetchall()]


def get_items_fetchall(sql_request):
    connection = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="root",
        db='airbnb'
    )
    cursor = connection.cursor()
    cursor.execute(sql_request)
    cursor.close()
    connection.close()
    return cursor.fetchall()


def liste_periodes_indispo(month):
    count_reservations = 0
    month_dispos = []
    month_reservations_liste = []
    month_reservations_count = 0
    month_reservationStarted = False
    month_reservationEnded = False
    month_reservationStarted_date = ""
    month_reservationEnded_date = ""
    for jour in month:
        if jour[2] in days:
            dispo = jour[3]
            month_dispos.append(dispo)
            if dispo == 'f':
                # Si indisponible et qu'il n'y a pas encore eu de réservations comptés
                if count_reservations == 0:
                    month_reservationStarted = True
                    month_reservationEnded = False
                    month_reservationStarted_date = jour[2]
                if month_reservationStarted:
                    count_reservations += 1
                # Si le jour est indispo et que c'est le dernier du mois, on termine la période d'indisponibilité
                if str(jour[2]) == days[-1] and month_reservationStarted:
                    count_reservations = 0
                    month_reservations_liste.append(
                        (month_reservationStarted_date, days[-1]))
                month_reservations_count += 1
            # Si le jour est disponible mais qu'on était déjà dans une période indisponible, on termine la période d'indisponibilité
            if dispo == 't' and month_reservationStarted:
                month_reservationStarted = False
                month_reservationEnded = True
                month_reservationEnded_date = month[month.index(
                    jour)-1][2]
            # Si on est plus dans une période d'indisponibilité et que la période calculée n'est pas dans la liste
            if month_reservationEnded and (month_reservationStarted_date, month_reservationEnded_date) not in month_reservations_liste:
                count_reservations = 0
                month_reservations_liste.append(
                    (month_reservationStarted_date, month_reservationEnded_date))
    return month_reservations_count,month_dispos, month_reservations_liste


lock = Lock()

year = constants.YEAR
print("Script pour l'année", year)

# Liste des annonces
listing_ids = get_items_list(
    "select distinct listing_id from "+constants.AIRBNB+" order by listing_id")

# Liste des dates de scraping
tab_date = get_items_list(
    "select distinct scraping_date from "+constants.AIRBNB+" order by scraping_date")

periodes_indispo_annee = {}
for listing_id in listing_ids:
    periodes_indispo_annee[listing_id] = []

# On fait l'opération pour chaque mois de 2018 (1 à 12)
for month in range(1, 13):
    # On filtre les dates de scraping pour n'avoir que celles qui sont avant et pour le mois lui-même
    tab_date_month = []
    for scraping_date in tab_date:
        y = scraping_date.split('-')[0]
        m = scraping_date.split('-')[1]
        if y != str(constants.YEAR):
            tab_date_month.append(scraping_date)
        else:
            if int(m) <= month:
                tab_date_month.append(scraping_date)

    num_days = calendar.monthrange(year, month)[1]
    days = [datetime.date(year, month, day).strftime("%Y-%m-%d")
            for day in range(1, num_days+1)]

    interpretation = {}

    def interpretation_remplissage(listing_id):
        global interpretation
        global periodes_indispo_annee

        i = 0
        j = 1

        lock.acquire()
        interpretation[listing_id] = {}
        interpretation[listing_id]["Details"] = {}
        lock.release()

        temp_month1 = {}  # Pour stocker le mois 2 et ne pas avoir à le recharger via SQL en mois 1 lors de la prochaine comparaison

        # Liste des véritables périodes indisponibles
        synthese_periodes_indisponibles = [0]*num_days

        # Remplissage de l'interprétation de chaque mois de scraping (différence d'un scraping à l'autre, nb de jours réservés, etc.)
        while j < len(tab_date_month):
            # on construit une paire de deux dates de scraping
            paire = [tab_date[i], tab_date[j]]

            # On remplit chaque tableau avec les données des scrapings correspondant
            if temp_month1 == {}:
                month1 = get_items_fetchall("select * from "+constants.AIRBNB+" where listing_id=" +
                                            str(listing_id)+" and scraping_date='" +
                                            paire[0]+"' order by scraping_date, date")
            else:
                month1 = temp_month1

            month2 = get_items_fetchall("select * from "+constants.AIRBNB+" where listing_id=" +
                                        str(listing_id)+" and scraping_date='" +
                                        paire[1]+"' order by scraping_date, date")
            temp_month1 = month2

            # On génère les listes des périodes indisponibles pour les 2 mois de scraping comparés (selon les jours fermé sur le calendrier)
            month1_dispos = []
            month2_dispos = []
            month1_reservations_liste = []
            month2_reservations_liste = []            
            month1_reservations,month1_dispos, month1_reservations_liste = liste_periodes_indispo(
                month1)
            month2_reservations,month2_dispos, month2_reservations_liste = liste_periodes_indispo(
                month2)

            # On remplit le dictionnaire interpetation pour la comparaison des deux fichiers de scraping
            lock.acquire()
            interpretation[listing_id][str(paire)] = {}
            lock.release()
            # S'il n'y a pas de changements
            if month1_reservations == month2_reservations:
                lock.acquire()
                interpretation[listing_id][str(paire)]["Total"] = str(
                    month1_reservations)+" jours indisponibles"
                lock.release()
            # Si on a des des jours indispo en plus
            if month1_reservations < month2_reservations:
                lock.acquire()
                interpretation[listing_id][str(paire)]["Total"] = "+ "+str(
                    month2_reservations-month1_reservations)+" jours indisponibles"
                lock.release()
            # Si on a des jours indispo en moins
            if month1_reservations > month2_reservations:
                lock.acquire()
                interpretation[listing_id][str(paire)]["Total"] = "- "+str(
                    month1_reservations-month2_reservations)+" jours indisponibles"
                lock.release()

            # Détails changement
            annulations = 0
            reservations = 0
            for dispo, dispo2 in zip(month1_dispos, month2_dispos):
                if dispo == 't' and dispo2 == 'f':
                    reservations += 1
                if dispo == 'f' and dispo2 == 't':
                    annulations += 1

            lock.acquire()
            interpretation[listing_id][str(
                paire)]["Reservations"] = reservations
            interpretation[listing_id][str(paire)]["Annulations"] = annulations
            interpretation[listing_id]["Details"][str(
                paire[0])] = month1_reservations_liste
            interpretation[listing_id]["Details"][str(
                paire[1])] = month2_reservations_liste
            lock.release()

            i = j
            j += 1

        # Remplissage du tableau synthese_periodes_indisponibles
        compteur_general = 1
        for scraping in tab_date_month:
            lock.acquire()
            month_reservations = interpretation[listing_id]["Details"][str(
                scraping)]
            lock.release()
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

        # Construction de la liste des périodes distinctes
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

        # On ajoute les périodes indispo du mois dans la liste des périodes indispo pour toute l'année
        for periode in liste_periodes:
            lock.acquire()
            periodes_indispo_annee[listing_id].append(periode)
            lock.release()

        # Liste périodes distinctes au format string pour l'affichage dans le JSON
        liste_periodes_string = []
        for periode in liste_periodes:
            liste_periodes_string.append((periode[0].strftime(
                "%Y-%m-%d"), periode[1].strftime("%Y-%m-%d")))
        if liste_periodes != [(datetime.date(year, month, 1), datetime.date(year, month, num_days))]:
            lock.acquire()
            interpretation[listing_id]["Synthese"] = {
                "Jours indisponibles": compteur_day,
                "Nombre de reservations distinctes": compteur_periodes,
                "Liste": liste_periodes_string
            }
            lock.release()
        # Si le propriétaire a fermé tout le mois
        else:
            if compteur_day == num_days:
                lock.acquire()
                interpretation[listing_id]["Synthese"] = "Le proprietaire a ferme tout le mois."
                lock.release()
            if compteur_day == 0:
                lock.acquire()
                interpretation[listing_id]["Synthese"] = "Le proprietaire n'a pas eu de reservations."
                lock.release()

    print("Mois",month,"- Remplissage de l'interprétation des calendriers")
    with ThreadPoolExecutor(max_workers=130) as executor:
        results = list(
            tqdm(executor.map(interpretation_remplissage, listing_ids), total=len(listing_ids)))

    print("Mois",month,"- Création de results_"+str(month)+".json")
    # Quand tous les threads ont terminé de remplir le dico interpretation
    with open('results_'+str(month)+'.json', 'w') as outfile:
        json.dump(interpretation, outfile)
    print("Fait.")
    print()

# Liaison avec les commentaires de l'annonce
def reviews_link(listing_id):
    global periodes_indispo_annee
    global tab_date
    
    liaisons = {}
    liaisons["Synthese"] = {}

    lock.acquire()
    liste_periodes = periodes_indispo_annee[listing_id]
    lock.release()

    if liste_periodes != [(datetime.date(year, month, 1), datetime.date(year, 12, calendar.monthrange(year, 12)[1]))] and liste_periodes != []:
        # On récupère les commentaires à la date de scraping la plus récente
        lock.acquire()
        last_scraping_date = tab_date[-1]
        lock.release()

        commentaires = get_items_fetchall("select listing_id, scraping_date,date,reviewer_id,reviewer_name,comments from "+constants.AIRBNB+" where listing_id=" +
                                          str(listing_id)+" and scraping_date='"+str(last_scraping_date)+"' order by date")
        # ETAPE 1 : commentaires dans les 15 jours après la réservation
        liaisons["Synthese"]["Nombre total de periodes"] = len(liste_periodes)
        for periode in liste_periodes:
            liaisons[str(periode)] = {}
            for commentaire in commentaires:
                date_du_commentaire = datetime.datetime.strptime(
                    commentaire[2], "%Y-%m-%d").date()
                if periode[1] < date_du_commentaire and periode[1] > date_du_commentaire-datetime.timedelta(days=14):
                    liaisons[str(periode)][commentaire[4]] = {
                        "date": commentaire[2],
                        "reviewer_id": commentaire[3],
                        "reviewer_name": commentaire[4],
                        "contenu": commentaire[5]
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
            if nb_resa > 0:
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

        def nettoyage_only_percent(pourcentage):
            id_a_clean = []
            for periode in liste_periodes:
                # Pour chaque période, on cherche si on a un commentaire unique à 50%
                for commentaire in liaisons[str(periode)]:
                    if liaisons[str(periode)][commentaire]["pourcentage"] == pourcentage and len(liaisons[str(periode)]) == 1:
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

        # ETAPE INTERMEDIAIRE : Nettoyage commentaire à 50% tout seul
        nettoyage_only_percent(50)

        # Count nombre de commentaires à 100% (avant l'étape 5 obligatoire)
        count_100percent = 0
        for periode in liste_periodes:
            # Pour chaque période, on cherche si un des commentaires est à 100%
            for commentaire in liaisons[str(periode)]:
                if liaisons[str(periode)][commentaire]["pourcentage"] == 100:
                    count_100percent += 1
        # On l'ajoute à results.json dans la partie Synthèse de l'annonce
        liaisons["Synthese"]["Nombre de periodes validees a 100%"] = count_100percent

        # ETAPE 5 : Entre deux commentaires à 50% (ou 33%, 25%, etc.) pour une période, on garde le commentaire le plus proche en date donc le premier
        count_closest = 0
        for x in range(2, 6):
            pourcentage = (1/x)*100
            continuer = True
            liste_id_a_conserver = []
            while continuer:
                continuer = False
                id_a_conserver = None
                periode_du_com = None
                temp = {}
                for periode in liste_periodes:
                    temp2 = {}
                    for commentaire in liaisons[str(periode)]:
                        if id_a_conserver == None and liaisons[str(periode)][commentaire]["pourcentage"] == pourcentage:
                            id_a_conserver = commentaire
                            if id_a_conserver not in liste_id_a_conserver:
                                liste_id_a_conserver.append(id_a_conserver)
                            periode_du_com = str(periode)
                            # temp contient le commentaire à conserver dans la première période où il se trouve
                            temp[commentaire] = liaisons[str(
                                periode)][commentaire]
                            continuer = True
                        if id_a_conserver != None and commentaire != id_a_conserver and str(periode) != periode_du_com:
                            # temp2 contient les autres commentaires de cette période
                            temp2[commentaire] = liaisons[str(
                                periode)][commentaire]
                    if id_a_conserver != None:
                        if str(periode) == periode_du_com:
                            liaisons[str(periode)] = temp
                        else:
                            liaisons[str(periode)] = temp2
                # Passage des commentaires maintenant uniques à 100%
                nettoyage_only_percent(pourcentage)
            count_closest += len(liste_id_a_conserver)

        if count_closest > 0:
            liaisons["Synthese"]["Nombre de periodes validees par le commentaire le plus proche"] = count_closest

        # Count periodes sans commentaires de moins de 21 jours
        count_sans_com = 0
        for periode in liste_periodes:
            # Pour chaque période, on cherche s'il n'y a pas de commentaire
            nb_jours = periode[1].day-periode[0].day
            if len(liaisons[str(periode)]) == 0 and nb_jours < 21:
                count_sans_com += 1
        liaisons["Synthese"]["Nombre de periodes validees sans commentaires de moins de 21 jours"] = count_sans_com

    if liaisons != {}:
        with open('liaisons_'+str(listing_id)+'.json', 'w') as outfile:
            json.dump(liaisons, outfile)

print("Liaisons avec les commentaires et génération des fichiers liaions_listing_id.json")
with ThreadPoolExecutor(max_workers=130) as executor:
    results = list(
        tqdm(executor.map(reviews_link, listing_ids), total=len(listing_ids)))
