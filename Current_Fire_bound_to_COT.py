import json
import xml.etree.ElementTree as ET
from arcgis.gis import GIS  # pip install arcgis (https://developers.arcgis.com/python/guide/intro/)
from datetime import datetime, timedelta
import socket
import ssl
import time
import os
import uuid
import requests
from arcgis.geometry import Geometry
from arcgis.geometry.functions import simplify, generalize
from pyproj import Transformer

OUTPUT_DIR = r"path\\to\\Fire COT"

CERT_FILE = r"path\\to\\\\user.crt.pem"  # path to your cert
KEY_FILE = r"path\\to\\user.key.pem"  # path to your key

TAK_IP = "00.00.000.00"  # specify your IP
TAK_PORT = ####  # specify port

def unescape(s):
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    s = s.replace("&amp;", "&")
    return s



def fetch_fire_data():
    try:
        # Connect to the ArcGIS Online account
        gis = GIS(url="https://geospatial.alberta.ca/portal")
        item = gis.content.get("0b775584ff2e4e2a8f0689a339614258")
        feature_layer = item.layers[3]
        
        # Query features and retrieve attributes
        fire_date_filter = '2024-01-30'
        query = f"CAPTURE_DATE >= '{fire_date_filter}'"
        features = feature_layer.query(where=query, return_geometry=True)
        
        if not features:
            raise ValueError("No features retrieved from query.")
        
        print(f"Retrieved {len(features)} features")
        
        # Extract geometries
        geometries = [feature.geometry for feature in features]
        

        # Define the spatial reference
        spatial_reference = 3400
        
        # Generalize geometries with a specified tolerance (e.g., 10 units)
        tolerance = 100  # You can adjust the tolerance value as needed
        generalized_geometries = generalize(geometries=geometries, spatial_ref=spatial_reference, max_deviation=tolerance, deviation_unit='')
        
        # Update features with simplified and generalized geometries
        for feature, generalized_geometry in zip(features, generalized_geometries):
            feature.geometry = generalized_geometry
        
        print(f"Simplified {len(features)} features")
        return features

    except Exception as e:
        print(f"Error: {e}")
        return []



def construct_cot_message(features):
    print("Constructing CoT messages...")

    now = datetime.utcnow()
    twenty_four_hours_from_now = now + timedelta(minutes=1440)
    cot_messages = []

    # Set up the transformer to convert from EPSG:3400 to EPSG:4326 (WGS 84)
    transformer = Transformer.from_crs("EPSG:3400", "EPSG:4326", always_xy=True)

    # Assign your field map properties:
    for feature in features:
        attributes = feature.attributes

        try:
            # Extract attributes
            uid = attributes["OBJECTID"]
            callsign = attributes.get("FIRE_NUMBE", "Unknown")
            hae = '9999999.0'
            ce = '9999999.0'
            le = '9999999.0'
            time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            start = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            stale = twenty_four_hours_from_now.strftime("%Y-%m-%dT%H:%M:%SZ")
            Fire_Number = attributes.get("FIRENUMBER", "Unknown")
            Fire_Label = attributes.get("FIRE_NUMBE", "Unknown")
            Fire_Class = attributes.get("FIRE_CLASS", "Unknown")
            Burn_Code = attributes.get("BURNCODE", "Unknown")
            Burn_Class = attributes.get("BURN_CLASS", "Unknown")
            Area = attributes.get("HECTARES_UTM", 0.0)
            Fire_Year = attributes.get("YEAR", "Unknown")
            Fire_Name = attributes.get("ALIAS", "Unknown")
            Capture_Date = attributes.get("CAPTURE_DATE", "Unknown")
            Capture_Time = attributes.get("TIME", "Unknown")
            Data_Source = attributes.get("SOURCE", "Unknown")

            # Convert Burn_Code based on patterns
            if Burn_Code and isinstance(Burn_Code, str):
                if "B" in Burn_Code:
                    Burn_Code = "Burned"
                elif "PB" in Burn_Code:
                    Burn_Code = "Partially Burned"
                elif "I" in Burn_Code:
                    Burn_Code = "Unburned Island"
                else:
                    Burn_Code = "Unknown"

            # Remarks will show in the info pane 
            remarks_text = f"""Fire Number: {Fire_Number}
                Fire Label: {Fire_Label}
                Fire Class: {Fire_Class}
                Burn Code: {Burn_Code}
                Burn Class: {Burn_Class}
                Area in ha: {Area}
                Fire Year: {Fire_Year}
                Fire Name: {Fire_Name}
                Capture Date: {Capture_Date}
                Capture Time: {Capture_Time}
                Data Source: {Data_Source}"""

            # Create XML element for the CoT message
            event = ET.Element("event")
            event.set("version", "2.0")
            event.set("uid", str(uid))  
            event.set("type", "u-d-f")
            event.set("time", str(time))
            event.set("start", str(start))
            event.set("stale", str(stale))
            event.set("how", "h-e")

                    
            # Create point element
            if feature.geometry and 'rings' in feature.geometry:

                   
                first_point = feature.geometry['rings'][0][0]
                lon, lat = transformer.transform(first_point[0], first_point[1])
                point = ET.SubElement(event, "point")
                point.set("lat", str(lat))
                point.set("lon", str(lon))
                point.set("hae", hae)
                point.set("ce", ce)
                point.set("le", le)
            else:
                print(f"Skipping feature {uid} due to missing geometry data.")


            # Create detail element
            detail = ET.SubElement(event, "detail")

            strokeColor = ET.SubElement(detail, "strokeColor")
            strokeColor.set("value", "-65536")

            strokeWeight = ET.SubElement(detail, "strokeWeight")
            strokeWeight.set("value", "1.0")

            fillColor = ET.SubElement(detail, "fillColor")

            # Adjust fill color based on Burn_Code
            if Burn_Code == 'Burned':
                fillColor.set("value", "-2147483648")
            elif Burn_Code == 'Partially Burned':
                fillColor.set("value", "-2139654281")
            else:
                fillColor.set("value", "-2130706433")

            strokeStyle = ET.SubElement(detail, "strokeStyle")
            strokeStyle.set("value", "solid")

            contact = ET.SubElement(detail, "contact")
            contact.set("callsign", callsign)

            precisionlocation = ET.SubElement(detail, "precisionlocation")
            precisionlocation.set("altsrc", "DTED0")

            remarks = ET.SubElement(detail, "remarks")
            remarks.text = remarks_text

            # Add geometry element for the polygon
            if feature.geometry and 'rings' in feature.geometry:
                

                # Extract and convert polygon coordinates
                rings = feature.geometry['rings']
                for ring_index, ring in enumerate(rings):
                    if ring_index == 0:
                        # Exterior ring
                        for coord in ring:
                            lon, lat = transformer.transform(coord[0], coord[1])
                            link = ET.SubElement(detail, "link")
                            link.set("point", f"{lat}, {lon}")


            cot_messages.append(event)

        except Exception as e:
            print(f"Error processing feature {attributes['OBJECTID']}: {e}")

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

    ssl_sock.close()
    print("All messages sent successfully")

def main():
    while True:
        try:
            # Fetch and process data
            features = fetch_fire_data()
            if not features:
                print("No features retrieved. Skipping processing.")
                continue

            cot_messages = construct_cot_message(features)
            if not cot_messages:
                print("No CoT messages constructed. Skipping sending.")
                continue

            send_cot_messages(cot_messages)
            save_cot_messages(cot_messages)
            print("Messages sent. Restarting the script in 24 hours...")

            # Calculate time until next run
            now = datetime.now()
            next_run = now + timedelta(days=1)
            next_run = next_run.replace(hour=0, minute=0, second=0, microsecond=0)
            sleep_time = (next_run - now).total_seconds()
            time.sleep(sleep_time)

        except Exception as e:
            print(f"Error in main loop: {e}")
            print("Restarting the script in 1 hour...")
            time.sleep(3600)

if __name__ == "__main__":
    main()
