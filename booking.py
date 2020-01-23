import json
import sqlite3

dict_months = {
    "janvier": "01",
    "février": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "août": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "décembre": "12"
}

f = open("booking_comments.txt","w+")
with open('booking.txt') as json_file:
    data = json.load(json_file)
    for d in data:
        conn = sqlite3.connect('AirBnB.db')
        c = conn.cursor()
        sql = "select listing_id from links where booking =" + "'" + str(d['id']) + "'"
        c.execute(sql)
        tab =[x[0] for x in c.fetchall()]
        if len(tab) > 0 :
            listing_id = tab[0]
            print(listing_id)
            print("---------")
            for elt in d['dates']:
                if len(elt) > 20:
                    elt = elt.replace("Commentaire envoyé le ", '')
                    t = elt.split(' ')
                    if len(t[0]) == 1:
                        day = "0" + t[0]
                    else:
                        day = t[0]

                    months = dict_months[t[1]]
                    year = t[2]
                    format_date = year + "-" + months + "-" + day

                    line = str(listing_id) + "," + "" + "," + str(format_date) + "," + "" + "," + "" + "," + "Comment from Booking" 
                    f.write(line + "\n")
                    
        print(" ")

    
      

       

        
        