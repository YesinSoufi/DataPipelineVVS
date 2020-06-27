import requests
import time
import pykafka
from pykafka import KafkaClient

client = KafkaClient(hosts="localhost:9092")
topic = client.topics['vvs']
producer = topic.get_sync_producer()

bhf = ["de:08111:6118","de:08111:6008","de:08111:6333", "de:08116:2103", "de:08111:6157","de:08116:7800", "de:08118:7402"]

hauptbahnhof = "de:08111:6118"
universitaet = "de:08111:6008"
flughafen = "de:08116:2103"
badcannstatt = "de:08111:6333"
feuerbach = "de:08111:6157"
esslingen = "de:08116:7800"
ludwigsburg = "de:08118:7402"
headers = {'User-Agent': 'Fiddler', 'Host': 'efastatic.vvs.de', 'content-type': 'text/xml',
           "Keep-Alive": "timeout=20, max=100"}
while True:
    ts = time.localtime()
    currenttime = time.strftime("%Y-%m-%dT%H:%M:%S", ts)

    for stops in bhf:
        xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <Trias version="1.1" xmlns="http://www.vdv.de/trias" xmlns:siri="http://www.siri.org.uk/siri">
        <ServiceRequest>
        <siri:RequestTimestamp>2020-06-18T14:00:00</siri:RequestTimestamp>
        <siri:RequestorRef>hdm0419</siri:RequestorRef>
        <RequestPayload>
        <StopEventRequest>
        <Location>
        <LocationRef>
        <StopPointRef>''' + stops + '''</StopPointRef>
        </LocationRef>
        <DepArrTime>''' + currenttime + '''</DepArrTime>
        </Location>
        <Params>
        <NumberOfResults>1</NumberOfResults>
        <StopEventType>departure</StopEventType>
        <PtModeFilter>
        <Exclude>false</Exclude>
        <RailSubmode>suburbanRailway</RailSubmode>
        </PtModeFilter>
        <IncludeRealtimeData>true</IncludeRealtimeData>
        </Params>
        </StopEventRequest>
        </RequestPayload>
        </ServiceRequest>
        </Trias>'''
        r = requests.post("http://efastatic.vvs.de/hdmstuttgart/trias", data=xml, headers=headers).text
        print (stops)
        message = r.encode()
        print ("Encoder fertig")
        producer.produce(message)
        print("Ist im Producer")


    time.sleep(60)
