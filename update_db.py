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
import io
import requests

if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists(os.path.join('data', 'temp')):
    os.makedirs(os.path.join('data', 'temp'))

conn = sqlite3.connect(os.path.join('data', 'fundos.db'))
db = conn.cursor()


# %% functions
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
    db.execute("CREATE INDEX idx_quotas_cnpj ON quotas(cnpj);")
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
    response = requests.get(file_url)
    df = pd.read_csv(io.BytesIO(response.content), sep=';', header=0, encoding='latin-1')
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'cnpj_fundo': 'cnpj'})

    # drop inactive
    df = df[df['sit'] == 'EM FUNCIONAMENTO NORMAL']
    # drop closed
    df = df[df['condom'] == 'Aberto']
    # drop no equity
    df = df[df['vl_patrim_liq'] != 0]
    df = df.drop_duplicates(subset=['cnpj'], keep='last')

    df = df[['cnpj', 'denom_social', 'classe', 'rentab_fundo', 'taxa_perfm', 'taxa_adm']]
    df[['taxa_perfm', 'taxa_adm']] = df[['taxa_perfm', 'taxa_adm']].fillna(value=0)
    db.execute("DELETE FROM inf_cadastral")
    df.to_sql('inf_cadastral', conn, if_exists='append', index=False)
    conn.commit()
    return


def update_quotes():
    """
    Updates the mutual funds quotes.

    Returns
    -------
    None.

    """

    db_files = pd.read_sql("SELECT * FROM files", conn)
    urls = ['http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/',
            'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/']
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
                    last_modified = pd.to_datetime(t.text[24:40]).date()
                else:
                    file_name = t.text[0:22]
                    last_modified = pd.to_datetime(t.text[22:38]).date()
                files[i] = {'file_name': file_name, 'url_date': last_modified}
                i += 1
    available_files = pd.DataFrame.from_dict(files, orient='index')
    new_files = available_files.merge(db_files, how='left', right_on='file_name', left_on='file_name')
    new_files = new_files.fillna(pd.to_datetime('1900-01-01'))
    new_files = new_files[new_files['url_date'] > pd.to_datetime(new_files['last_modified'])]

    for idx, file in new_files.iterrows():
        if len(file['file_name']) == 22:
            url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/'
            zip_or_csv = 'zip'
        else:
            url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
            zip_or_csv = 'csv'
        file_url = url + file['file_name']
        file_data = requests.get(file_url).content
        db.execute(f"""DELETE FROM files
                        WHERE file_name = '{file['file_name']}'""")
        load_file(file_data, zip_or_csv=zip_or_csv)
        db.execute(f"""INSERT INTO files 
                        VALUES ('{file['file_name']}', '{file['url_date']}')""")
        print(f"{file['file_name']} downloaded successfully.")
        conn.commit()

    return


def load_file(file_data, zip_or_csv):
    """
    Loads the file with the new quotes.

    Parameters
    ----------
    file_name : string

    Returns
    -------
    None.

    """

    active = pd.read_sql("SELECT cnpj FROM inf_cadastral", conn)['cnpj']

    if zip_or_csv == 'zip':
        zip_file = zipfile.ZipFile(io.BytesIO(file_data))
        # dict with all csv files
        files_dict = {}
        for i in range(len(zip_file.namelist())):
            files_dict[zip_file.namelist()[i]] = zip_file.read(zip_file.namelist()[i])
    else:
        files_dict = {'any_name': file_data }

    for key in files_dict.keys():
        df = pd.read_csv(io.BytesIO(files_dict[key]), sep=';', header=0, encoding='latin-1')
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'cnpj_fundo': 'cnpj', 'dt_comptc': 'date', 'vl_quota': 'quota'})
        df = df[df['cnpj'].isin(list(active))]
        df = df[['cnpj', 'date', 'quota']]
        year = df['date'].str[:4].unique()[0]
        month = df['date'].str[5:7].unique()[0]
        db.execute(f"""DELETE FROM quotas 
                       WHERE SUBSTR(date, 1, 4) = '{year}' AND 
                             SUBSTR(date, 6, 2) = '{month}'""")
        df.to_sql('quotas', conn, if_exists='append', index=False)
        conn.commit()

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
    for file in available_files:
        if file not in db_files:
            for fl in os.listdir(os.path.join('data', 'temp')):
                os.remove(os.path.join('data', 'temp', fl))
            file_url = f"ftp://ftp.cetip.com.br/MediaCDI/{file}"
            wget.download(file_url, os.path.join('data', 'temp'))
            with open(os.path.join('data', 'temp', file), 'r') as content:
                cdi = int(content.readline()) / 100

            d_factor = ((cdi / 100) + 1) ** (1 / 252)
            date = datetime.datetime.strptime(file[:8], '%Y%m%d')
            db.execute(f"""INSERT INTO cdi 
                            VALUES ('{date}', {cdi}, {d_factor})""")

            # These files are not updated by the provider (cetip.com.br).
            # Because of that, the last_modified is not important, and set to 1900-01-01
            db.execute(f"""INSERT INTO files 
                            VALUES ('{file}', '1900-01-01')""")
            conn.commit()
            print("CDI file " + file + " downloaded successfully.")
    return


def update_pipeline():
    # create database tables
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='quotas';"
    if db.execute(query).fetchone() == None:
        create_tables()

    update_register()
    update_quotes()
    update_cdi()
    return




