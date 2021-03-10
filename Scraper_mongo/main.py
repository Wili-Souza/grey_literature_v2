from bs4 import BeautifulSoup
import requests
import pandas as pd
from copy import deepcopy
from time import sleep

from server.connection import db

#--- Meus imports
from scrum_scraper import scrum_scraper as scrum
from AA_scraper import agileAlliance_scraper as agileAlliance
from AC_scraper import agileConnection_scraper as agileConnection
from exportar import exportar_df

scraped_data = { # -> dicionário para data frame 
    'tipo': [],
    'titulo': [], 
    'link': [],
    'data': [],
    'descricao': [],
    'autor': []
}


def eliminate_duplicates(data):
    #Eliminando repetições de posts em data
    idx_duplicates = [i for i in range(0, len(data['titulo'])) if data['titulo'][i] in data['titulo'][i+1:]]
    for i in range(len(data['titulo']) - 1, -1, -1):
        if i in idx_duplicates:
            for key in data:
                del data[key][i]

# Insere conteúdo nas novas postagens 
def insert_content(data, AC=False):
    #---------  Fazendo scraping para adicionar conteúdo dos posts
    for j in range(len(data['titulo']) -1, -1, -1):
        print(j)

        #Pegando conteudo do post
        try:
            html = requests.get(data['link'][j])
            soup = BeautifulSoup(html.text, 'html.parser')
        except:
            print('Erro ao conectar-se com página do conteúdo, tentando novamente...')
            print('Link: {}' .format(data['link'][j]))
            break

        content = ''
            
        if AC:
            div_of_parag = soup.select_one('div.field-item.even')
            div_of_summary = soup.select_one('div.summary')
            if div_of_summary == None:
                div_of_summary = soup.select_one('div.field.field-name-body')

            if div_of_parag is not None:
                paragraphs = div_of_parag.find_all('p')
            if div_of_summary is not None:
                paragraphs += div_of_summary.find_all('p')

            if paragraphs is not None:
                for p in paragraphs:
                    content += p.text
        else:
            paragraphs = soup.find_all('p')

            if paragraphs is not None:
                for p in paragraphs:
                    content += p.text
        
        #Inserindo conteúdo no dicionário
        data['descricao'][j] = content.lower()

    return data

def search_on_scrum():
    scraped_data_scrum = deepcopy(scraped_data)

    #Pesquisando todos os resultados existentes
    temp_dict = scrum()

    #Inserindo resultados no dicionário 
    for key in scraped_data_scrum:
        if key == 'autor':
            scraped_data_scrum[key] = ['' for x in range(0, len(scraped_data_scrum['titulo']))]
            continue
        scraped_data_scrum[key] += temp_dict[key]

    eliminate_duplicates(scraped_data_scrum)

    scraped_data_scrum = insert_content(scraped_data_scrum)

    print('FIM DA EXECUÇÃO NO SCRUM ...')

    return scraped_data_scrum

def search_on_AA():
    scraped_data_AA = deepcopy(scraped_data)

    #Pesquisando todos os resultados existentes
    temp_dict = agileAlliance()

    #Inserindo no dicionario
    for key in scraped_data_AA:
        scraped_data_AA[key] += temp_dict[key]

    scraped_data_AA = insert_content(scraped_data_AA)

    print('FIM DA EXECUÇÃO NO AGILE ALLIANCE ...')

    return scraped_data_AA

def search_on_AC():
    scraped_data_AC = deepcopy(scraped_data)

    #Pesquisando todos os resultados existentes
    temp_dict = agileConnection()

    #inserindo no dicionário
    for key in scraped_data_AC:
        scraped_data_AC[key] += temp_dict[key]

    eliminate_duplicates(scraped_data_AC)

    scraped_data_AC = insert_content(scraped_data_AC)

    print('FIM DA EXECUÇÃO NO AGILE CONNECTION ...')

    return scraped_data_AC

# verifica se os novos posts possuem tipos novos (se sim, adiciona a lista de tipos)
def search_new_types(new_data):
    types_saved = [x['value'] for x in db.types.find()]
    for post in new_data:
        if post['tipo'].strip() not in types_saved:
            db.types.insert_one({'value': post['tipo'].strip()})

# --- Chamando funções de busca
scraped_data_AC = search_on_AC()
scraped_data_scrum = search_on_scrum()
scraped_data_AA = search_on_AA()

# --- convertendo dicionários em json (vai ser simplificado ***)
new_data_AA = [{"tipo":a, "titulo":b, "link":c, "data":d, "descricao": e, "autor":f} for a, b, c, d, e, f in \
            zip(scraped_data_AA["tipo"], scraped_data_AA["titulo"], scraped_data_AA["link"], scraped_data_AA["data"], \
            scraped_data_AA["descricao"], scraped_data_AA["autor"])]

new_data_AC = [{"tipo":a, "titulo":b, "link":c, "data":d, "descricao": e, "autor":f} for a, b, c, d, e, f in \
            zip(scraped_data_AC["tipo"], scraped_data_AC["titulo"], scraped_data_AC["link"], scraped_data_AC["data"], \
            scraped_data_AC["descricao"], scraped_data_AC["autor"])]

new_data_S = [{"tipo":a, "titulo":b, "link":c, "data":d, "descricao": e, "autor":f} for a, b, c, d, e, f in \
            zip(scraped_data_scrum["tipo"], scraped_data_scrum["titulo"], scraped_data_scrum["link"], scraped_data_scrum["data"], \
            scraped_data_scrum["descricao"], scraped_data_scrum["autor"])]


# --- Enviando atualizações para collections do banco de dados

if len(new_data_AA) > 0:
    print("AA -> {}" .format(len(new_data_AA)))
    db.postsAA.insert_many(new_data_AA)
    search_new_types(new_data_AA)

if len(new_data_AC) > 0:
    print("AC ->  {}" .format(len(new_data_AC)))
    db.postsAC.insert_many(new_data_AC)
    search_new_types(new_data_AC)

if len(new_data_S) > 0:
    print("S ->  {}" .format(len(new_data_S)))
    db.postsS.insert_many(new_data_S)
    search_new_types(new_data_S)
