# -- coding: utf-8 --
import xml.etree.ElementTree as ET
import time
from datetime import datetime
import mysql.connector
import mysql
import pykafka
from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092")
topic = client.topics['vvs']

consumer = topic.get_simple_consumer()
for message in consumer:
    if message is not None:
        print (message.value.decode())
        message = message.value.decode()

        root = ET.fromstring(message)


        # DEBUG für XML Probleme
        count = 0
        for child in root[0][5][0][1][1][1]:
            print(child.tag + str(count))
            count = count + 1
        # ElementTree für die Geplante Abfahrtszeit
        timtabletime = root[0][5][0][1][1][0][0][3][0].text + "h"

        # ElementTree für die Linie
        if root[0][5][0][1][1][1][5].tag == '{http://www.vdv.de/trias}PublishedLineName':
            linie = root[0][5][0][1][1][1][5][0].text
        elif root[0][5][0][1][1][1][6].tag == '{http://www.vdv.de/trias}PublishedLineName':
            linie = root[0][5][0][1][1][1][6][0].text
        else:
            print("Probleme in der XML-Antwort: PublishedLineName ist weder Child Nr. 5 oder 6")

        # ElementTree für die Richtung
        if root[0][5][0][1][1][1][11].tag == '{http://www.vdv.de/trias}DestinationText':
            richtung = root[0][5][0][1][1][1][11][0].text
        elif root[0][5][0][1][1][1][12].tag == '{http://www.vdv.de/trias}DestinationText':
            richtung = root[0][5][0][1][1][1][12][0].text
        elif root[0][5][0][1][1][1][10].tag == '{http://www.vdv.de/trias}DestinationText':
            richtung = root[0][5][0][1][1][1][10][0].text
        else:
            print("Probleme bei der XML-Antwort: DestinationText ist weder Child Nr. 11 noch Nr.12 noch Nr.10")

        # Element Tree für die prognostizierte Abfahrtszeit
        if root[0][5][0][1][1][0][0][3][1].tag == '{http://www.vdv.de/trias}EstimatedTime':
            estimatedtime = root[0][5][0][1][1][0][0][3][1].text + "h"
        else:
            print("Probleme bei der Echtzeit-Erfassung auf dem Server")

        # ElementTree für die Haltestelle
        haltestelle = root[0][5][0][1][1][0][0][1][0].text

        # Berechnung der Verspätung, Erfassung des Datums und Erstellung des Doppelcodes (ID)
        if len(estimatedtime) & len(timtabletime) == 21:
            estdt = datetime.strptime(estimatedtime, '%Y-%m-%dT%H:%M:%SZh')
            tbldt = datetime.strptime(timtabletime, '%Y-%m-%dT%H:%M:%SZh')
            diff = (estdt-tbldt)
            seconds = diff.total_seconds()
            minutes = seconds/60
            verspaetung = int(minutes)
            tbl1 = timtabletime.split("T")
            tbl2 = tbl1[1].split("Z")
            est1 = estimatedtime.split("T")
            est2 = est1[1].split("Z")
            ts = time.localtime()
            currentdate = time.strftime("%Y-%m-%d", ts)
            datecode = time.strftime("%Y%m%d", ts)
            doppelcode = str(datecode + tbl2[0] + linie + richtung)



        """for child in root[0][5][0][1][1][1]:
            print(child.tag)"""



        print (timtabletime)
        print (estimatedtime)
        print (linie)
        print (richtung)
        print(haltestelle)

        connection = mysql.connector.connect(host="localhost", user="root", passwd="root", db="vss")
        print ("Connection ist da")
        curser = connection.cursor()
        print ("Curser ist da")


        curser = connection.cursor()
        curser.execute("INSERT IGNORE INTO vvsdaten (id,datum,estimated,timetabled,verspaetung,haltestelle) VALUES (%s,%s,%s,%s,%s,%s)",(doppelcode,currentdate,est2[0],tbl2[0],verspaetung,haltestelle))
        curser.close()
        connection.commit()
        print ("In Datenbank geschrieben")