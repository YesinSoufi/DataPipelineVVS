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
        timtabletime = root[0][5][0][1][1][0][0][3][0].text+"h"
        linie = root[0][5][0][1][1][1][5][0].text
        richtung = root[0][5][0][1][1][1][11][0].text
        estimatedtime = root[0][5][0][1][1][0][0][3][1].text+"h"
        haltestelle = root[0][5][0][1][1][0][0][1][0].text

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