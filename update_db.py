# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 19:15:26 2020

@author: Diego
"""

import pandas as pd
import sqlite3
import wget
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup
import urllib.request
import datetime
import zipfile

cwd = os.getcwd()
if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('data\\b3_data'):
    os.makedirs('data\\b3_data')
if not os.path.exists('data\\temp'):
    os.makedirs('data\\temp')    
conn = sqlite3.connect(cwd + '\\data\\fundos.db')
db = conn.cursor()

#%% functions
def create_tables():
    """
    Creates all tables in the database.

    Returns
    -------
    None.

    """
    db.execute("""CREATE TABLE IF NOT EXISTS files
                   (file_name TEXT,
                    last_modified DATE)""")
    db.execute("""CREATE TABLE IF NOT EXISTS quotas
                   (cnpj TEXT, 
                    date DATE, 
                    quota REAL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS inf_cadastral
                   (cnpj TEXT,
                   denom_social TEXT,
                   classe text,
                   rentab_fundo TEXT,
                   taxa_perfm INTEGER,
                   taxa_adm REAL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS cdi
                   (date DATE,
                    cdi REAL,
                    d_factor REAL)""")
                   
def update_register():
    """
    Updates the mutual funds register.

    Returns
    -------
    None.

    """
    url = 'http://dados.cvm.gov.br/dados/FI/CAD/DADOS/'
    files = {}
    i = 0 
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table')
    tr = table.find_all('tr')
    for t in tr:
        if t.text[0:17] == 'inf_cadastral_fi_':
            file_name = t.text[0:29]
            last_modified = pd.to_datetime(t.text[29:45])
            files[i] = {'file_name': file_name, 'url_date': last_modified}
            i += 1
    available_files = pd.DataFrame.from_dict(files, orient='index')  
    available_files['url_date'] = pd.to_datetime(available_files['url_date'])
    last_file = available_files['file_name'][available_files['url_date'] == max(available_files['url_date'])].values[0]
    file_url = f"http://dados.cvm.gov.br/dados/FI/CAD/DADOS/{last_file}"
    for file in os.listdir(cwd+'\\data\\temp\\'):
        os.remove(cwd+f'\\data\\temp\\{file}')
    file_name = wget.download(file_url, cwd+'\\data\\temp\\')
    df = pd.read_csv(cwd+'\\data\\temp\\'+last_file, sep=';', header=0, encoding='latin-1')
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'cnpj_fundo': 'cnpj'})
    df = df[['cnpj', 'denom_social', 'classe', 'rentab_fundo', 'taxa_perfm', 'taxa_adm']]
    df[['taxa_perfm', 'taxa_adm']] = df[['taxa_perfm', 'taxa_adm']].fillna(value=0)
    db.execute("DELETE FROM inf_cadastral")
    df.to_sql('inf_cadastral', conn, if_exists='append', index=False)
    return

def update_quotes():
    """
    Updates the mutual funds quotes.

    Returns
    -------
    None.

    """
    for fl in os.listdir(cwd+'\\data\\temp\\'):
        os.remove(cwd+'\\data\\temp\\'+fl)
    db_files = pd.read_sql("SELECT * FROM files", conn)
    db_files['last_modified'] = pd.to_datetime(db_files['last_modified'])
    urls = ['http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/', 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
    files = {}
    i = 0   
    for url in urls:
        html = urlopen(url)
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find('table')
        tr = table.find_all('tr')
        for t in tr:
            if t.text[0:14] == 'inf_diario_fi_':
                if url == 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/':
                    file_name = t.text[0:24]
                    last_modified = pd.to_datetime(t.text[24:40])
                else:
                    file_name = t.text[0:22]
                    last_modified = pd.to_datetime(t.text[22:38])
                files[i] = {'file_name': file_name, 'url_date': last_modified}
                i += 1
    available_files = pd.DataFrame.from_dict(files, orient='index')
    new_files = available_files.merge(db_files, how='left', right_on='file_name', left_on='file_name')
    new_files = new_files.fillna(pd.to_datetime('1900-01-01'))
    new_files = new_files[new_files['url_date'] > new_files['last_modified']]
    os.chdir(cwd+'\\data\\temp')
    for idx, file in new_files.iterrows():
        if len(file['file_name']) == 22:
            url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/'
        else:
            url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
        file_url = url + file['file_name']
        file_name = wget.download(file_url)
        db.execute(f"""DELETE FROM files
                        WHERE file_name = '{file['file_name']}'""")
        load_file(file_name)
        db.execute(f"""INSERT INTO files 
                        VALUES ('{file['file_name']}', '{file['url_date']}')""")
        print(f"{file['file_name']} downloaded successfully.")
        conn.commit()
    os.chdir(cwd)
    return
    
def load_file(file_name):
    """
    Loads the file with the new quotes.

    Parameters
    ----------
    file_name : string

    Returns
    -------
    None.

    """
    if file_name[-4:] == '.zip':
        with zipfile.ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(path=cwd+'\\data\\temp\\')
        os.remove(cwd+'\\data\\temp\\'+file_name)
    for fl in os.listdir(cwd+'\\data\\temp\\'):
        df = pd.read_csv(cwd+'\\data\\temp\\'+fl, sep = ';', header = 0, encoding = 'latin-1')
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'cnpj_fundo': 'cnpj', 'dt_comptc': 'date', 'vl_quota': 'quota'})
        df = df[['cnpj', 'date', 'quota']]
        year = df['date'].str[:4].unique()[0]
        month = df['date'].str[5:7].unique()[0]
        db.execute(f"""DELETE FROM quotas 
                       WHERE SUBSTR(date, 1, 4) = '{year}' AND 
                             SUBSTR(date, 6, 2) = '{month}'""")
        df.to_sql('quotas', conn, if_exists='append', index=False)
        os.remove(cwd+'\\data\\temp\\'+fl)
    return

def update_cdi():
    """
    Updates the CDI (Brazilian reference rate).

    Returns
    -------
    None.

    """
    # Files in the ftp:
    url = 'ftp://ftp.cetip.com.br/MediaCDI/'
    req = urllib.request.Request(url)
    r = urllib.request.urlopen(req)
    text = str(r.read())
    text = text.replace('\\n', ' ')
    text = text.replace('\\r', '')
    text = text.replace("b'", "")
    text = text.replace("'", "")
    text = text.split()
    available_files = []
    for file_name in text:
        if file_name[-4:] == '.txt':
            available_files.append(file_name)
    # Files in the database:
    db_files = pd.read_sql("SELECT * FROM files", conn)
    db_files = db_files['file_name'].to_list()
    # check if the file is new, process and update files table
    new_files = {}
    i = 0
    for file in available_files:
        if file not in db_files:
            new_files[i] = {'file_name': file, 'last_modified': '1900-01-01'}
            file_url = f"ftp://ftp.cetip.com.br/MediaCDI/{file}"
            file_name = wget.download(file_url, cwd+f'\\data\\b3_data\\{file}')
            print("CDI file "+file+" downloaded successfully.")
            i += 1
    new_files = pd.DataFrame.from_dict(new_files, orient='index')
    if len(new_files) > 0:
        cdi = {}
        i = 0
        for file in new_files['file_name']:
            content = open(cwd+'\\data\\b3_data\\'+file, 'r')
            date = datetime.datetime.strptime(file[:8], '%Y%m%d')
            cdi[i] = {'date': date, 'cdi': int(content.readline())}
            i += 1
        cdi = pd.DataFrame.from_dict(cdi, orient='index')
        cdi['cdi'] = cdi['cdi']/100
        cdi['d_factor'] = ((cdi['cdi']/100)+1)**(1/252)
        cdi.to_sql('cdi', conn, if_exists='append', index=False)
        new_files.to_sql('files', conn, if_exists='append', index=False)
    return

def update_pipeline():
    create_tables()
    update_register()
    update_quotes()
    update_cdi()
    return




