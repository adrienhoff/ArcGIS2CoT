import json
import xml.etree.ElementTree as ET
from arcgis.gis import GIS
from datetime import datetime, timedelta
import socket
import ssl
import time
import requests
import os

OUTPUT_DIR = r"G:\\PY\\Fire COT"

CERT_FILE = r"\\Path\\to\\your\\crt.pem" #path to your cert
KEY_FILE = r"\\Path\\to\\your\\key.pem" #path to your key

TAK_IP = "00.00.000.00" #specify your IP
TAK_PORT = 8086 #specify port


def unescape(s):
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    s = s.replace("&amp;", "&")
    return s

def fetch_fire_data():
    # URL of the feature service
    url = "https://cfs.geohub.sa.gov.au/server/rest/services/CFS_Incident_Read/CFS_Incidents/FeatureServer/0/query"
    # Parameters to query features
    params = {
        "f": "json",  # Specify output format as JSON
        "where": "1=1",  # SQL-like where clause, here retrieving all features
        "outFields": "*",  # Specify which fields to include, "*" means all fields
        "returnGeometry": True  # Specify whether to return geometry
        # You can add more parameters as needed, such as spatial filters or result pagination
    }
    # Send request to get features
    response = requests.get(url, params=params)
    data = response.json()
    features = data["features"]
    return features


def construct_cot_message(features):
    print("Constructing CoT messages...")

    now = datetime.utcnow()
    twentyfour_hrs_from_now = now + timedelta(minutes=1440)
    cot_messages = []

    for feature in features:
        attributes = feature["attributes"]
        # Extract attributes
        uid = feature["attributes"]["id"]  
        callsign = feature["attributes"]["incident_name"]  
        lat = feature["attributes"]["lat"]  
        lon = feature["attributes"]["long"]
        hae = '9999999.0'
        ce = '9999999.0'
        le ='9999999.0'
        time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        start = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        stale = twentyfour_hrs_from_now.strftime("%Y-%m-%dT%H:%M:%SZ")
        remarks_url = 'https://apps.geohub.sa.gov.au/CFSMap/index.html'  
        INCIDENT_NAME = feature["attributes"]["incident_name"]
        NAME = feature["attributes"]["name"]
        REPORTED = feature["attributes"]["first_report"] 
        STATUS = feature["attributes"]["status"] 
        REGION = feature["attributes"]["region"] 
        AIRCRAFT = feature["attributes"]["aircraft"] 
        ICON = feature["attributes"]["icon"] 
        EVENT = feature["attributes"]["event"] 
        
        
        remarks_text = """
        INCIDENT NAME: {}
        NAME: {}
        FIRST REPORTED: {}
        STATUS: {}
        REGION: {}
        AIRCRAFT: {}
        ICON: {}
        EVENT: {}""".format(INCIDENT_NAME, NAME, REPORTED, STATUS, REGION, AIRCRAFT, ICON, EVENT)


        # Create XML element for the CoT message
        event = ET.Element("event")
        event.set("version", "2.0")
        event.set("uid", str(uid))
        event.set("type", "a-n-G")
        event.set("time", str(time))
        event.set("start", str(start))
        event.set("stale", str(stale))
        event.set("how", "h-g-i-g-o")

        # Create point element
        point = ET.SubElement(event, "point")
        point.set("lat", str(lat))
        point.set("lon", str(lon))
        point.set("hae", str(hae))
        point.set("ce", str(ce))
        point.set("le", str(le))

        # Create detail element
        detail = ET.SubElement(event, "detail")
        link = ET.SubElement(detail, "link")
        link.set("url", remarks_url)
        link.set("mime", "text/html")
        link.set("relation", "r-u")
        link.set("uid", str(uid))
        link.set("remarks", "LINK TO MAP")

        # Add usericon element
        usericon = ET.SubElement(detail, "usericon")
        
        if 'Fire' in ICON:
            usericon.set("iconsetpath", "f7f71666-8b28-4b57-9fbb-e38e61d33b79/Google/firedept.png")
        elif 'Burn' in ICON:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Human Caused Hazards/Hazard--Fire-Forest.png")
        elif 'Flood' in ICON:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Natural Hazards/Hazard--Flood.png")
        elif 'Hazmat' in ICON:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Hazards/Hazard--Fire--Radioactive.png")
        elif 'Marine' in ICON:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Incident/LZ--Marine-Dock.png")
        elif 'Structure' in ICON:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Human Caused Hazards/Hazard--Fire-Commercial.png")
        elif 'Vehicle' in ICON:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Transportation/Amtrak-Bus-Station.png")
        else:
            usericon.set("iconsetpath", "de450cbf-2ffc-47fb-bd2b-ba2db89b035e/Hazards/Hazard--Fire--General-Hazards.png")


        color = ET.SubElement(detail, "color")
        color.set("argb", "-1")  # use Google default red color
        
        
        contact = ET.SubElement(detail, "contact")
        contact.set("callsign", callsign)

        precisionlocation = ET.SubElement(detail, "precisionlocation")
        precisionlocation.set("altsrc", "DTED0")

        remarks = ET.SubElement(detail, "remarks")
        remarks.text = remarks_text

        cot_messages.append(event)
        
    print(f"Constructed {len(cot_messages)} CoT messages")
    return cot_messages

def save_cot_messages(cot_messages):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    for i, cot_message in enumerate(cot_messages):
        cot_message_xml = ET.tostring(cot_message, encoding='utf-8', method='xml')
        filename = os.path.join(OUTPUT_DIR, f"cot_message_{i+1}.cot")
        with open(filename, "wb") as file:
            file.write(cot_message_xml)
        print(f"Saved message to {filename}")

def send_cot_messages(cot_messages):
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssl_sock = context.wrap_socket(sock, server_hostname=TAK_IP)

    ssl_sock.connect((TAK_IP, TAK_PORT))

    for cot_message in cot_messages:
        cot_message_xml = ET.tostring(cot_message, encoding='utf-8', method='xml')
        ssl_sock.sendall(cot_message_xml)
        print("Sent message: ", cot_message_xml)

    ssl_sock.close()
    print("All messages sent successfully")


def main():
    while True:
        try:
            # Fetch and process data
            features = fetch_fire_data()
            cot_messages = construct_cot_message(features)
            send_cot_messages(cot_messages)
            #save_cot_messages(cot_messages)
            print("Messages sent. Restarting the script in 24 hours...")
            
            # Calculate the time until the next run (next day at the same time), if loaded into nssm this will continue running as a process
            now = datetime.now()
            next_run = now + timedelta(days=1)
            next_run = next_run.replace(hour=0, minute=0, second=0, microsecond=0)
            time_to_sleep = (next_run - now).total_seconds()
            
        except Exception as e:
            print(f"Error: {e}")
            # If an error occurs, try again in 30 min
            time_to_sleep = 30 * 60
        
        # Sleep until the next run
        time.sleep(time_to_sleep)


if __name__ == "__main__":
    main()
