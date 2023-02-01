from bs4 import BeautifulSoup
import requests
import boto3
import re
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

#df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_february_us_airport_traffic.csv')
#df['text'] = df['airport'] + '' + df['city'] + ', ' + df['state'] + '' + 'Arrivals: ' + df['cnt'].astype(str)

# initializing the headers and webpage url  
#headers3 = ({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
url = "https://www.ncei.noaa.gov/access/homr/file/nexrad-stations.txt"

# recording the response from the webpage
response = requests.get(url)
#print(type(response.text))
lines = response.text.split('\n')
i=1
nexrad=[]
for line in lines:
    line=line.strip()
    word_list = line.split(" ")
    if (word_list[-1].upper() == 'NEXRAD'):
        nexrad.append(line)
        #print(word_list[-1])
    #print(i.strip()[-1:-6])
nexrad = [i for i in nexrad if 'UNITED STATES' in i]
print(len(nexrad))
satellite_metadata = {
        'state': [],
        'county': [],
        'latitude': [],
        'longitude': []
}
for satellite in nexrad:
    satellite = satellite.split("  ")
    #print(i)
    #i+=1
    satellite =  [i.strip() for i in satellite if i != ""]
    print(satellite)
    #satellite=satellite.strip()
    for i in range(len(satellite)):
        if (re.match(r'\b[A-Z][A-Z]\b',satellite[i].strip())):
            #state_county = satellite[i].split()
            satellite_metadata['state'].append(satellite[i][:2])
            satellite_metadata['county'].append(satellite[i][2:])

for satellite in nexrad:
    satellite = satellite.split(" ")
    satellite =  [i.strip() for i in satellite if i != ""]
    print(satellite)
    for i in range(len(satellite)):
        if (re.match(r'^-?[0-9]\d(\.\d+)?$',satellite[i])):
            state_county = satellite[i].split()
            satellite_metadata['latitude'].append(satellite[i])
            satellite_metadata['longitude'].append(satellite[i+1])
            break

    #if(flag == 1):
    #    satellite =  [i.split() for i in satellite]
    #    print(satellite)
    #    if (re.match(r'^-?[0-9]\d(\.\d+)?$' ,satellite[i])):
    #        lat_long = satellite[i].split()
    #        print(lat_long)
            #satellite_metadata['latitude'].append(lat_long[0])
            #satellite_metadata['longitude'].append(lat_long[1])
    #        break
print(len(satellite_metadata['state']))
print(satellite_metadata['county'])
print(len(satellite_metadata['latitude']))
print(len(satellite_metadata['longitude']))
    #for satellite in nexrad:
#text = satellite_metadata['county'] + ", " + satellite_metadata['state']
fig = go.Figure(data=go.Scattergeo(
        lon = satellite_metadata['longitude'],
        lat = satellite_metadata['latitude'],
        text = satellite_metadata['county'],
        mode = 'markers',
        #marker_color = df['cnt'],
        ))
fig.update_layout(
        title = 'Most trafficked US airports<br>(Hover for airport names)',
        geo_scope='usa',
    )
fig.show()