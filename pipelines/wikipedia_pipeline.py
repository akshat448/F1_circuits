import json

import pandas as pd
import pycountry_convert as pc
from geopy import Nominatim


#NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # check if the request is successful

        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable"})[0]

    table_rows = table.find_all('tr')

    return table_rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' *'):
        text = text.split(' *')[0]
    if text.find('[a]') or text.find('[b]') or text.find('[c]'):
        text = text.split('[a]')[0].split('[b]')[0].split('[c]')[0]
    #text = re.sub(r'\[\w\]', '', text)
    #if text.find(' (formerly)') != -1:
    #    text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)
    data = []

    for i in range(1, len(rows)):
        
        tds = rows[i].find_all('td')
        values = {
            'circuit': clean_text(tds[0].text),
            'map': 'https://' + tds[1].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'type': clean_text(tds[2].text),
            'direction': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'country': clean_text(tds[5].text),
            'last_length_used': clean_text(tds[6].text),
            'turns': clean_text(tds[7].text),
            'grand_prix': clean_text(tds[8].text),
            'seasons': clean_text(tds[9].text),
            'grand_prix_held': clean_text(tds[10].text)
        }
        data.append(values)

    #df = pd.DataFrame(data)
    #df.to_csv('data/output.csv', index=False)
    
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    return "OK"


def get_lat_long(city, country):
    geolocator = Nominatim(user_agent='http')
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

    return None


def country_to_continent(country_name):
    try:
        # Convert the country name into country code
        country_code = pc.country_name_to_country_alpha2(country_name, cn_name_format="default")
        # Convert the country code into continent code
        continent_name = pc.country_alpha2_to_continent_code(country_code)
        return continent_name
    except:
        return 'Unknown'


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    circuit_df = pd.DataFrame(data)
    circuit_df['location_track'] = circuit_df.apply(lambda x: get_lat_long(x['country'], x['circuit']), axis=1)
    circuit_df['region'] = circuit_df['country'].apply(country_to_continent)
    #circuit_df['maps'] = circuit_df['maps'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)


    # handle the duplicates
    duplicates = circuit_df[circuit_df.duplicated(['location_track'])]
    duplicates['location_track'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    circuit_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=circuit_df.to_json())

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('circuits_' + str(datetime.now().date())+'.csv')

    data.to_csv('data/' + file_name, index=False)