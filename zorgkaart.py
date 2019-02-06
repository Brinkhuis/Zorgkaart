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
    base_url = 'https://www.zorgkaartnederland.nl/overzicht/organisatietypes'
    r = requests.get(base_url)
    soup = BeautifulSoup(r.content, 'html.parser')
    items = soup.find('section', {'class': 'content_section'}).find_all('li')
    
    organisatietype = [list(item.stripped_strings)[0] for item in items]
    organisatietype_id = [item.a['href'][1:] for item in items]
    aantal = [int(list(item.stripped_strings)[1][1:-1]) for item in items]
     
    return pd.DataFrame({'organisatietype_id': organisatietype_id,
                         'organisatietype': organisatietype,
                         'aantal': aantal}).set_index('organisatietype_id')


def get_typeid(organisatietype):
    all_types = get_types()
    return all_types.loc[all_types['organisatietype'] == organisatietype].index[0]


def get_info(organisatietype, reasonable_rate = 3):

    # set base_url
    base_url = 'https://www.zorgkaartnederland.nl'
    
    # get organisatietype_id
    organisatietype_id = get_typeid(organisatietype)

    # pagination
    r = requests.get(f'{base_url}/{organisatietype_id}')
    soup = BeautifulSoup(r.content, 'html.parser')
    pagination = soup.find('div', {'class': 'pagination_holder'})
    if len(pagination.find_all('li')) == 0:
        pages = 1
    else:
        pages = int(pagination.find_all('li')[-1].text.strip())

    # expected number of datapoints
    datapoints_expected = int(soup.title.text.split()[0])

    # datastore
    instelling = pd.DataFrame()

    # scrape pages
    for page in tqdm(range(1, pages + 1)):
        
        r = requests.get(f'{base_url}/{organisatietype_id}/pagina{page}')
        soup = BeautifulSoup(r.content, 'html.parser')
        locaties = soup.find_all('li', {'class': 'media'})

        for locatie in locaties:
            
            latitude = float(locatie['data-location'].split(',')[0])
            longitude = float(locatie['data-location'].split(',')[1])
            zorginstelling = locatie.a['title']
            zknl_url = base_url + locatie.a['href']
            identifier = int(locatie.a['href'].split('-')[-1])
            beoordeling = locatie.find_all('div')[1].div.text
            waarderingen = locatie.find('span', {'class': 'rating_value'}).text
            categorie = locatie.find('p', {'class': 'description'}).text
            plaats = locatie.find('span', {'class': 'context'}).text
            
            table_row = [identifier, zorginstelling, plaats, beoordeling, waarderingen, categorie, latitude, longitude, zknl_url]
            instelling = instelling.append(pd.Series(table_row), ignore_index=True)
        
        if reasonable_rate:
            sleep(randint(1, reasonable_rate)) 

    # post processing
    instelling.columns = ['id', 'zorginstelling', 'plaats', 'beoordeling', 'waarderingen', 'categorie', 'latitude', 'longitude', 'zknl_url']
    instelling.id = instelling.id.astype(int)
    instelling.waarderingen = instelling.waarderingen.astype(int)
    instelling.beoordeling.replace('-', np.nan, inplace=True)
    instelling.beoordeling = instelling.beoordeling.astype(float)
    instelling.set_index('id', drop=True, inplace=True, verify_integrity=True)

    # save data
    if not os.path.isdir('data'):
        os.mkdir('data')
    filename = organisatietype.lower().replace('-', '_').replace(' ', '_') + '_info.csv'
    instelling.to_csv(os.path.join('data', filename))

    # validate results
    if instelling.shape[0] != datapoints_expected:
        print(f'Check results: {instelling.shape[0]} datapoints found ({datapoints_expected} expected)')
    
    return instelling


def read_info(organisatietype):
    
    filename = organisatietype.lower().replace('-', '_').replace(' ', '_') + '_info.csv'

    try:
        instelling = pd.read_csv(os.path.join('data', filename), index_col='id')
        return instelling
    except FileNotFoundError as E:
        print(E)


def get_details(organisatietype, reasonable_rate = 3):

    # set base_url
    base_url = 'https://www.zorgkaartnederland.nl'
    
    # get organisatietype_id
    organisatietype_id = get_typeid(organisatietype)
    
    # read data
    instelling = read_info(organisatietype)

    # datastore
    instelling_details = pd.DataFrame()
    
    # scrape pages
    for page in tqdm(instelling.zknl_url):

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
                website = row[1]
        except IndexError:
            pass
        
        try:
            row = list(address_rows[3].stripped_strings)
            if row[0] == 'Telefoon':
                telefoon = row[1]
            if row[0] == 'Website':
                website = row[1]
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

    # save data
    if not os.path.isdir('data'):
        os.mkdir('data')
    filename = organisatietype.lower().replace('-', '_').replace(' ', '_') + '_details.csv'
    instelling_details.to_csv(os.path.join('data', filename)) 
        
    return instelling_details


def merge_datasets(organisatietype, delete=False):

    dataset_info = read_info(organisatietype)

    try:
        filename = organisatietype.lower().replace('-', '_').replace(' ', '_') + '_details.csv'
        dataset_details = pd.read_csv(os.path.join('data', filename), index_col='id')
    except FileNotFoundError as E:
        print(E)
    
    dataset_merged = pd.merge(dataset_info, dataset_details, how='left', on='id')
    
    dataset = pd.DataFrame({'id': dataset_merged.index, 'zorginstelling': dataset_merged.zorginstelling_y, 'adres': dataset_merged.adres, 'postcode': dataset_merged.postcode, 'plaats': dataset_merged.plaats_y, 'telefoon': dataset_merged.telefoon, 'beoordeling': dataset_merged.beoordeling, 'waarderingen': dataset_merged.waarderingen, 'categorie': dataset_merged.categorie, 'latitude': dataset_merged.latitude, 'longitude': dataset_merged.longitude, 'website': dataset_merged.website, 'zorgkaart_url': dataset_merged.zknl_url, 'wachttijden_url': dataset_merged.wachttijden_url})
    dataset.set_index('id', drop=True, inplace=True, verify_integrity=True)

    filename = organisatietype.lower().replace('-', '_').replace(' ', '_') + '.csv'
    dataset.to_csv(os.path.join('data', filename))
    
    if delete:
        filename = organisatietype.lower().replace('-', '_').replace(' ', '_')
        os.remove(os.path.join('data', f'{filename}_info.csv'))
        os.remove(os.path.join('data', f'{filename}_details.csv'))
    
    return dataset


def get_wachttijden(organisatietype, reasonable_rate = 3):
    filename = organisatietype.lower().replace('-', '_').replace(' ', '_')
    if f'{filename}.csv' in os.listdir('data'):
        instelling = pd.read_csv(os.path.join('data', f'{filename}.csv'))
    elif f'{filename}_details.csv' in os.listdir('data'):
        instelling = pd.read_csv(os.path.join('data', f'{fielename}_details.csv'))
    else:
        raise Exception('Geen bestanden voor organisatietype {} gevonden'.format(organisatietype))
    
    instelling = instelling.loc[pd.notnull(instelling.wachttijden_url), ['id', 'zorginstelling', 'wachttijden_url']]
    
    if instelling.shape[0] != 0:
        wachttijden = pd.DataFrame()
    
        for row in instelling.iterrows():
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
        
        filename = organisatietype.lower().replace('-', '_').replace(' ', '_')
        wachttijden.to_csv(os.path.join('data', f'{filename}_wachttijden.csv'), index=False)

        return wachttijden
    else:
        print(f'Geen wachttijden gevonden voor organisatietype {organisatietype}')


if __name__ == '__main__':
    import sys
    get_info(sys.argv[1])
    get_details(sys.argv[1])
    merge_datasets(sys.argv[1], delete=True)
