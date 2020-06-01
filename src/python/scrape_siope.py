import os, ssl
import re
import numpy as np
import pandas as pd
import requests
import urllib.request
from bs4 import BeautifulSoup
from lxml import html

# Scrape with not valid SSL certificate
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
    getattr(ssl, '_create_unverified_context', None)): 
    ssl._create_default_https_context = ssl._create_unverified_context

def data_from_given_city(URL, city_code, colnames, len_items):

    '''
    Returns a pandas dataframe with data extracted from SIOPE Website
    for a given IBGE city code.

    Parameters:

    URL: URL to extract tables
    city_code: city code from IBGE
    colnames: colnames to be passed to dataframe
    len_items: n# of columns with data
    '''
    URL = '{0}{1}'.format(URL, city_code)
    PAGE = urllib.request.urlopen(URL).read()
    soup = BeautifulSoup(PAGE)

    tables = soup.find_all('table', {'class': 'table'})
    list_of_tables = []

    for i in tables:
        storeTable = i.find_all('tr')
        tabledata = []
        finaldata = []

        for row in storeTable:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            tabledata.append([ele for ele in cols if ele])

        for item in tabledata:
            if len(item) > len_items:
                for row in range(len(item)):
                    item[row] = re.sub('\xa0%','', item[row])
                    item[row] = re.sub(',','.', item[row])
                finaldata.append(item)    

        list_of_tables.append(finaldata)

    data = pd.DataFrame([item for sublist in list_of_tables for item in sublist], 
                        columns=colnames)
    
    data['city_code'] = str(city_code)

    return(data)

# Get data from Years 2008 and 2016
data_slice_01 = data_from_given_city(
        URL='https://www.fnde.gov.br/siope/indicadoresFinanceirosEEducacionais.do?acao=PESQUISAR&anoPaginacao=2011&paginacao=-&pag=result&cod_uf=31&municipios=',
        city_code=317130, 
        colnames=['id', 'var_name', 'y2008', 'y2009','y2010', 'y2011', 'y2012', 'y2013'], len_items=7)

data_slice_02 = data_from_given_city(URL='https://www.fnde.gov.br/siope/indicadoresFinanceirosEEducacionais.do?acao=PESQUISAR&anoPaginacao=2013&paginacao=%2B&pag=result&cod_uf=31&municipios=',
        city_code=317130,
        colnames=['id', 'var_name', 'y2014', 'y2015','y2016'],
        len_items=4)

data = data_slice_01.merge(data_slice_02, on=['id', 'var_name', 'city_code'])
data = data[ ['city_code'] + [ col for col in data.columns if col != 'city_code' ] ]

data.to_csv('~/Downloads/SIOPE.csv')
