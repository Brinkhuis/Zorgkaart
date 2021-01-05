import pandas as pd
import numpy as np
import os
import requests
import sys
from bs4 import BeautifulSoup
from random import randint
from time import sleep
from tqdm import tqdm


def get_types():
    base_url = 'https://www.zorgkaartnederland.nl'
    r = requests.get(base_url + '/overzicht/organisatietypes')
    soup = BeautifulSoup(r.content, 'html.parser')
        
    items = soup.find('div', {'class': 'search-list'}).find_all('a', href=True)
    
    organisatietype = [list(item.stripped_strings)[0] for item in items]
    aantal = [int(item.span.text.strip('()')) for item in items]
    url = [base_url + item['href'] for item in items]
    
    return pd.DataFrame({'organisatietype': organisatietype,
                         'aantal': aantal,
                         'url': url}).set_index('organisatietype')


def get_info(organisatietype, reasonable_rate = 3):
    
    # set base_url
    base_url = 'https://www.zorgkaartnederland.nl'
    
    # get url
    url = get_types().at[organisatietype, 'url']
    
    # pagination
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    pagination = soup.find('nav', {'aria-label': 'Page navigation'})
    
    if pagination is None:
        pages = 1
    else:
        page_items = pagination.find_all('li', {'class': 'page-item'})
        pages = int(page_items[-1].a.text)

    # expected number of datapoints
    datapoints_expected = int(soup.title.text.split()[0])

    # datastore
    instelling = pd.DataFrame()

    # scrape pages
    for page in tqdm(range(1, pages + 1), desc='Pagination'):
        
        r = requests.get(f'{url}/pagina{page}')
        soup = BeautifulSoup(r.content, 'html.parser')
        locaties = soup.find_all('div', {'class': 'filter-result'})

        for locatie in locaties:
            
            zorginstelling = locatie['data-title'].strip()
            plaats = locatie.find('div', {'class', 'filter-result__places'}).text.strip()
            beoordeling = locatie.find('div', {'class', 'filter-result__score'}).text.strip()
            waarderingen = locatie.find_all('p')[-1].text.strip().split()[0]
            latitude = float(locatie['data-location'].split(',')[0])
            longitude = float(locatie['data-location'].split(',')[1])
            zknl_url = base_url + locatie.a['href']
            id = zknl_url.split('-')[-1]
            
            table_row = [id, zorginstelling, plaats, beoordeling, waarderingen, latitude, longitude, zknl_url]
            instelling = instelling.append(pd.Series(table_row), ignore_index=True)
        
        if reasonable_rate:
            sleep(randint(1, reasonable_rate)) 

    # post processing
    instelling.columns = ['id', 'zorginstelling', 'plaats', 'beoordeling', 'waarderingen', 'latitude', 'longitude', 'zknl_url']
    instelling.id = instelling.id.astype(int)
    instelling.waarderingen = instelling.waarderingen.astype(int)
    instelling.beoordeling.replace('-', np.nan, inplace=True)
    instelling.beoordeling = instelling.beoordeling.astype(float)
    instelling.latitude = instelling.latitude.astype(float)
    instelling.longitude = instelling.longitude.astype(float)
    
    # validate results
    if instelling.shape[0] != datapoints_expected:
        print(f'Check results: {instelling.shape[0]} datapoints found ({datapoints_expected} expected)')
    
    # return data
    return instelling


def get_details(organisatietype, reasonable_rate = 3):

    # set base_url
    base_url = 'https://www.zorgkaartnederland.nl'
    
    # read data
    instelling_info = get_info(organisatietype, reasonable_rate)

    # datastore
    instelling_details = pd.DataFrame()
    
    # scrape pages
    for page in tqdm(instelling_info.zknl_url, desc=organisatietype):

        r = requests.get(page)
        soup = BeautifulSoup(r.content, 'html.parser')
        content = soup.body.find('div', {'id': 'body-content'})
        address_rows = content.find_all('div', {'class': 'address_row'})
        
        identifier = int(page.split('-')[-1])
        zorginstelling = soup.title.text.split(' - ')[0]
        adres = address_rows[0].span.text.strip()
        postcode = list(address_rows[1].stripped_strings)[0]
        plaats = list(address_rows[1].stripped_strings)[1]
        
        telefoon = np.nan
        website = np.nan
        
        try:
            row = list(address_rows[2].stripped_strings)
            if row[0] == 'Telefoon':
                telefoon = row[1]
            if row[0] == 'Website':
                website = row[1].rstrip('/')
        except IndexError:
            pass
        
        try:
            row = list(address_rows[3].stripped_strings)
            if row[0] == 'Telefoon':
                telefoon = row[1]
            if row[0] == 'Website':
                website = row[1].rstrip('/')
        except IndexError:
            pass

        wachttijden_url = np.nan
        tabs = content.find('ul', {'id': 'responsive_tabs'}).find_all('li')
        for tab in tabs:
            if tab.a.text == 'Wachttijden':
                wachttijden_url = base_url + tab.a['href']
        
        table_row = [identifier, zorginstelling, adres, postcode, plaats, telefoon, website, wachttijden_url]
        instelling_details = instelling_details.append(pd.Series(table_row), ignore_index=True)
        
        if reasonable_rate:
            sleep(randint(1, reasonable_rate)) 

    # post processing
    instelling_details.columns = ['id', 'zorginstelling', 'adres', 'postcode', 'plaats', 'telefoon', 'website', 'wachttijden_url']
    instelling_details.id = instelling_details.id.astype(int)
    instelling_details.set_index('id', drop=True, inplace=True, verify_integrity=True)

    # merge data
    df = pd.merge(instelling_info, instelling_details, how='left', on='id')
    dataset = pd.DataFrame({'id': df.index, 'zorginstelling': df.zorginstelling_y, 'adres': df.adres, 'postcode': df.postcode, 'plaats': df.plaats_y, 'telefoon': df.telefoon, 'beoordeling': df.beoordeling, 'waarderingen': df.waarderingen, 'latitude': df.latitude, 'longitude': df.longitude, 'website': df.website, 'zorgkaart_url': df.zknl_url, 'wachttijden_url': df.wachttijden_url})
    
    # return data
    return dataset


def get_wachttijden(organisatietype, reasonable_rate = 3):
    instelling = get_details(organisatietype, reasonable_rate)

    df = instelling.copy().reset_index()
    df = df.loc[pd.notnull(df.wachttijden_url), ['id', 'zorginstelling', 'wachttijden_url']]
    
    if df.shape[0] != 0:
        wachttijden = pd.DataFrame()
    
        for row in tqdm(df.iterrows(), total=df.shape[0], desc='Wachttijden'):
            row_values = row[1]
            r = requests.get(row_values.wachttijden_url)
            soup = BeautifulSoup(r.content, 'html.parser')
            specialismen = soup.find('ul', {'class': 'striped_box certificates_table'}).find_all('li')
            for specialisme in specialismen:
            
                wachttijd = specialisme.find('div', {'class': 'right_media_holder'}).text.strip()
                if wachttijd in ['-']:
                    wachttijd = np.nan
                else:
                    wachttijd = int(wachttijd.split()[0])
                
                specialisme = specialisme.find('div', {'class': 'media-body'}).text.strip()
            
                table_row = [row_values.id, row_values.zorginstelling, specialisme, wachttijd]
            
                wachttijden = wachttijden.append(pd.Series(table_row), ignore_index=True)
                
                if reasonable_rate:
                    sleep(randint(1, reasonable_rate)) 
    
        wachttijden.columns = ['id', 'zorginstelling', 'specialisme', 'wachttijd']
        wachttijden.id = wachttijden.id.astype(int)
        wachttijden.dropna(inplace=True)
        wachttijden.wachttijd = wachttijden.wachttijd.astype(int)
        
        return instelling, wachttijden 
    else:
        print(f'Geen wachttijden gevonden voor organisatietype: {organisatietype}')
        return instelling, None
