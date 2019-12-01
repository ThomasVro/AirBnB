import sqlite3
import json
import datetime
import calendar
year = 2018
month = 5
num_days = calendar.monthrange(year, month)[1]
days = [datetime.date(year, month, day).strftime("%Y-%m-%d")
        for day in range(1, num_days+1)]
conn = sqlite3.connect('listing_and_reviews_10annonces.db')


def sql_request(request):
    '''
    Execute une requête SQl sur notre base de donnée.

    Param :
        request (string) : Requête àréaliser sur la BDD.
    
    Return :
        c.fetchall() () : Données retourner par la requête.
    '''
    c = conn.cursor()
    c.execute(request)  
    return c.fetchall()

def create_temp_tab(num_days, month_reservations_dates, compteur_general):
    '''
    Return the temporary file for a scrapping month. (temp)

    Param :
        num_days (int) : nuber ofday in a mounth
        month_reservations_dates () : list on periodes for this scrapping mounth
        compteur_general : compteur a incrémenter entre chaque mois de scapping
    '''
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
    return temp

def compare_scapping_periode_to_synthese(synthese_periodes_indisponibles, temp):
    '''
    Compare le tableau de synthese avec celui temporaire de scrapping

    Param :
        synthese_periodes_indisponibles (list) : synthese de la liste des periodes réservé sur le mois.
        temp (list) : periodes reperé a une date de scrapping.
    Result : 
        synthese_periodes_indisponibles (list) : synthese de la liste des periodes résevé sur le mois (mis a jour).
        list_periodes (list) : list de toutes les periodes avec date de début date de fin en datetime.
        compteur_day (int) : décompte du nombre de jour indisponible dans le mois.
        compteur_periodes (in) : décompte du nombre de reservations différentes.
    '''
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
    return synthese_periodes_indisponibles, liste_periodes, compteur_day, compteur_periodes

def list_periodes_to_interpretation(interpretation, liste_periodes):
    '''
    Transforme les données de nos periodes en informations lisibles (ToString)

    Param :
        interpretation (dict) : Dictionnaire contenant toutes les informations interprété que nous voulons transmettre
        list_periodes (list) : liste de nos periodes sous format datetime.
    '''
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

def link_periodes_to_reviews(liste_periodes):
    '''
    Cette fonction a pour but de relier les periodes de reservation avec un commentaire pour valider la periode.

    Param :
        liste_periodes (list) : liste des periodes indisponible du mois.

    Return :
        liaisons (dict) : Liste des periodes avec pour chaque periodes les commentaires associés.
    '''

    liaisons = {}
    if liste_periodes != [(datetime.date(year, month, 1), datetime.date(year, month, num_days))] and liste_periodes != []:
        sql = sql_request("select * from listing_and_reviews where listing_id='" + \
            str(listing_id)+"' and scraping_date = '" + \
                str(tab_date[0])+"' and id not null order by date")
        commentaires = sql
        '''
        ETAPE 1 : Recherche des commentaire pour chaque periode
            On vérifie pour chaque periodes si il y a un commentaire dans les 15 jours suivant,
            (car un commentaire ne peut etre posté que dans les 15 jours suivant la fin de la reservation)
            Si c'est le cas on ajoute se commentaire a notre periode.
        '''
        for periode in liste_periodes:
            liaisons[str(periode)]={}
            for commentaire in commentaires:
                date_du_commentaire = datetime.datetime.strptime(
                commentaire[2], "%Y-%m-%d").date()
                if periode[1] < date_du_commentaire and periode[1] > date_du_commentaire-datetime.timedelta(days=14):                    
                    liaisons[str(periode)][commentaire[4]]={
                        "date":commentaire[2],
                        "reviewer_id":commentaire[5],
                        "reviewer_name":commentaire[6],
                        "contenu":commentaire[7]
                    }
        '''
        ETAPE 2 : Pourcentage de correspondance periodes / commentaire
            Pour chaque commentaire on compte le nombre de réservations qui se sont terminées 15 jours max avant
            afin de déterminer le pourcentage de chance qu'un commentaire corresponde à une periode.
        '''
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
                    liaisons[str(periode)][commentaire[4]]["pourcentage"]=(1/nb_resa)*100
        '''
        ETAPE 3 : retirer les commentaires de moins de 100% pour une réservation qui a déjà un commentaire à 100%
            On va parcourir les periodes a la recherche de commentaire a 100%
            Si c'est le cas nous allons :
                Retirer les commentaires qui sont a moins de 100%
                Recalculer les pourcantages de ces commentaires sur les autres periodes.
        '''
        continuer = True
        while continuer:
            continuer = False        
            for periode in liste_periodes:            
                found = False
                id_a_modif = []
                #Pour chaque période, on cherche si un des commentaires est à 100%
                for commentaire in liaisons[str(periode)]:
                    if liaisons[str(periode)][commentaire]["pourcentage"]==100:
                        id_Cent = commentaire
                        found = True
                    else:
                        id_a_modif.append(commentaire)
                #Si c'est le cas
                if found and id_a_modif!=[]:
                    print(periode)
                    print(id_a_modif)
                    temp = {}
                    #On ne laisse que le commentaire à 100% pour cette période
                    for commentaire in liaisons[str(periode)]:
                        if commentaire == id_Cent:
                            temp[commentaire] = liaisons[str(periode)][commentaire]
                    liaisons[str(periode)]=temp
                    #On met à jour les pourcentages des commentaires liés à d'autres périodes
                    for id in id_a_modif:
                        for periode in liste_periodes:
                            if id in liaisons[str(periode)]:
                                ancien_nb_resa = int(100/liaisons[str(periode)][id]["pourcentage"])
                                nouveau_pourcentage = (1/(ancien_nb_resa-1))*100
                                liaisons[str(periode)][id]["pourcentage"]= nouveau_pourcentage
                                if not continuer and nouveau_pourcentage == 100:
                                    continuer = True
    return liaisons













# liste des dates de scraping
sql = sql_request("select distinct scraping_date from listing_and_reviews")
tab_date = [x[0] for x in sql]

# liste des annonces
sql = sql_request("select distinct listing_id from listing_and_reviews order by listing_id")
listing_ids = [x[0] for x in sql]

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
        if temp_month1 == {}:
            sql = sql_request("select * from listing_and_reviews where listing_id='" + \
                str(listing_id)+"' and scraping_date='" + \
                paire[0]+"' order by scraping_date, date")
            month1 = sql
        else:
            month1 = temp_month1

        sql = sql_request("select * from listing_and_reviews where listing_id='" + \
            str(listing_id)+"' and scraping_date='" + \
            paire[1]+"' order by scraping_date, date")
        month2 = sql
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

        interpretation[listing_id]["Details"][str(paire[0])] = month1_reservations_liste
        interpretation[listing_id]["Details"][str(paire[1])] = month2_reservations_liste












        i = j
        j += 1

    compteur_general = 1
    for scraping in tab_date:
        month_reservations = interpretation[listing_id]["Details"][str(scraping)]
        month_reservations_dates = []
        for periode in month_reservations:
            periode_date = (datetime.datetime.strptime(periode[0], "%Y-%m-%d").date(), datetime.datetime.strptime(periode[1], "%Y-%m-%d").date())
            month_reservations_dates.append(periode_date)
        month_reservations_dates = sorted(month_reservations_dates, key=lambda x: x[0].day)

        #création du tableau temporaire pour le mois de scrapping
        temp = create_temp_tab(num_days,month_reservations_dates,compteur_general)


    #Comparaison du tab temp a la synthese.
    synthese_periodes_indisponibles, liste_periodes, compteur_day, compteur_periodes = compare_scapping_periode_to_synthese(synthese_periodes_indisponibles,temp) 


    #Transformation des données en information lisible en sortie.
    list_periodes_to_interpretation(interpretation,liste_periodes)
   

    # Liaison avec les commentaires de l'annonce
    liaisons = link_periodes_to_reviews(liste_periodes)
    
    with open('liaisons_'+str(listing_id)+'.json', 'w') as outfile:
        json.dump(liaisons, outfile)

with open('results.json', 'w') as outfile:
    json.dump(interpretation, outfile)
print("Fait.")
