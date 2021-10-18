import pandas as pd
import geopandas as gpd
import requests
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
import time
import logging
import os

logging.basicConfig(filename='geocoder.log', encoding='utf=8', level=logging.DEBUG)

from boto.s3.connection import S3Connection
mailchimp_api_key = S3Connection(os.environ['MAILCHIMP_API_KEY'])
eric_g_maps_key = S3Connection(os.environ['ERIC_GOOGLE_MAPS_KEY'])
mailchimp_list = S3Connection(os.environ['MAILCHIMP_LIST'])


boundaries = gpd.read_file('./Political Boundaries v1/LA Political Boundaries.shp')
cogs = gpd.read_file('./Regional_Council_Districts_–_SCAG_Region.geojson')
cogs = cogs[['SUBREGION', 'geometry']].rename(columns={'SUBREGION':'COG'})
senators = pd.read_csv('./senators.csv').set_index('District').fillna('')
assemblymembers = pd.read_csv('./assemblyJune21.csv')

try:
    client = MailchimpMarketing.Client()
    client.set_config({
        "api_key": mailchimp_api_key,
        "server": "us4"
      })

    response = client.lists.get_all_lists()
#   print(response)
except ApiClientError as error:
    logging.info("Error: {}".format(error.text))

sfa_list_id = response['lists'][0]['id']

merge_fields = {'NC': 'MMERGE17', 'LA City': 'MMERGE11', 'LA County': 'MMERGE12', 'State Asse': 'MMERGE13',
               'State Sena': 'MMERGE14', 'Fed House': 'MMERGE15', 'COG':'MMERGE16'}

def geocode (address, key=eric_g_maps_key):
    '''Geocode using Google Maps API (Eric's key)'''
    url = f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={key}'
    r = requests.get(url)
    return r.json()

def clean_la_cd(la_cd):
    try:
        return str(int(round(la_cd, (0))))
    except:
        return ''

def geocode_subscriber(subscriber_id):
    global subscriber
    global joined
    global geocoded
    subscriber = client.lists.get_list_member(list_id=mailchimp_list,
                                              subscriber_hash=subscriber_id)
    address_fields = subscriber['merge_fields']['ADDRESSYU']
    if not address_fields:
        logging.info(f'no address for: {subscriber["full_name"]}, {subscriber["email_address"]}')
        return
        ## quick fix for missing state info
#     print(address_fields)
    if (address_fields['zip'][0] == '9') and not address_fields['state']:
        address_fields['state'] = 'CA'
        logging.info('updating-->', end='')
    address = ' '.join(
            [address_fields['addr1'], address_fields['city'], address_fields['state'], address_fields['zip']])
    logging.info(address)

    geocoded = geocode(address)
    
    if geocoded['status'] in ('INVALID_REQUEST', 'ZERO_RESULTS'):
        return
    
    else:
        #create Geopandas GeoDataFrame for spatial join
        gdf = gpd.GeoDataFrame([geocoded['results'][0]['geometry']['location']])
        gdf.geometry = gpd.points_from_xy(gdf['lng'], gdf['lat'])
        gdf = gdf.set_crs('EPSG:4326')
        #spatial join geocoded address with shapefile, subset columns
        joined = gpd.sjoin(gdf, boundaries, how='left')[[
            'LA City', 'Fed House', 'State Sena', 'State Asse', 'NC', 'LA County', 'geometry']]
        #join in COGS
        joined = gpd.sjoin(joined, cogs, how='left')
        #clean fields/add empty strings if not in LA City or a COG area
        joined['LA City'] = joined['LA City'].fillna('')
        joined['LA City'] = joined['LA City'].apply(clean_la_cd)
        joined['NC'] = joined['NC'].fillna('')
        joined['COG'] = joined['COG'].fillna('')
        
        #add geocoded info to Mailchimp merge fields
        for field in merge_fields.keys():
            subscriber['merge_fields'][merge_fields[field]] = joined[field][0]
        #update subscriber info
        try:
            client.lists.update_list_member(
                list_id=mailchimp_list, subscriber_hash=subscriber_id, body=subscriber)
        except ApiClientError as error:
            logging.info("Error: {}".format(error.text))
        return 

for offset in range(0,1801,200):
    logging.info(offset)
    members = client.lists.get_list_members_info(mailchimp_list, count=200, offset=offset)
    for member in members['members']:
        try:
            geocode_subscriber(member['id'])
        except:
            logging.info(f'geocode failed for {member["full_name"]}')
            

def add_senator(subscriber_id):
    subscriber = client.lists.get_list_member(list_id=mailchimp_list,
                                          subscriber_hash=subscriber_id)
    district = subscriber['merge_fields']['CASENATED']
    if district == '':
        return
    senator = ' '.join(senators.loc[int(district)])
    subscriber['merge_fields']['CASENATOR'] = senator
        #update subscriber info
    try:
        client.lists.update_list_member(
            list_id=mailchimp_list, subscriber_hash=subscriber_id, body=subscriber)
    except ApiClientError as error:
        logging.info("Error: {}".format(error.text))
    return
    
for offset in range(0,1801,200):
    logging.info(offset)
    members = client.lists.get_list_members_info(mailchimp_list, count=200, offset=offset)
    for member in members['members']:
        time.sleep(.1)
        try:
            add_senator(member['id'])
        except:
            logging.info(f'failed for member {member["full_name"]}')
            
assemblymembers['Dist'] = assemblymembers['Dist'].apply(lambda x: x[:3]).astype('int64')
assemblymembers = assemblymembers.set_index('Dist')

def add_assembly(subscriber_id):
    subscriber = client.lists.get_list_member(list_id=mailchimp_list,
                                          subscriber_hash=subscriber_id)
    district = subscriber['merge_fields']['CAASSEMBLD']
    if district == '':
        return
    member = assemblymembers.loc[int(district), 'Member'].split(',')
    member.reverse()
    member = ' '.join(member)
    subscriber['merge_fields']['CAASSEMBLY'] = member
        #update subscriber info
    try:
        client.lists.update_list_member(
            list_id=mailchimp_list, subscriber_hash=subscriber_id, body=subscriber)
    except ApiClientError as error:
        logging.info("Error: {}".format(error.text))
    return
    
for offset in range(0,1801,200):
    logging.info(offset)
    members = client.lists.get_list_members_info(mailchimp_list, count=200, offset=offset)
    for member in members['members']:
        time.sleep(.1)
        try:
            add_assembly(member['id'])
        except:
            logging.info(f'could not add assemblymember for member {member["full_name"]}, id:{member["id"]}')

os.system('shutdown -h now')
