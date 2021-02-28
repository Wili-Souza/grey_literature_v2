from bs4 import BeautifulSoup
import requests
import pandas as pd
from server.connection import db

collection = db.postsS
all_titles = [x['titulo'] for x in list(collection.find({}))]

#  ---- Minhas funções e classes
from conversaoData import converterData
from search import searchScrum
from exportar import exportar_df

data = { # -> diciionário para data frame 
    'tipo': [],
    'titulo': [],
    'link': [],
    'data': [],
    'descricao': []
}

def scrum_scraper():
    print("Iniciando execuação no Scrum ...")
    for page in range(0, 9999):
        break_next = False

        try:
            html = requests.get('https://www.scrum.org/resources?trainer_only_enable=0&page={}' .format(str(page)))
            soup = BeautifulSoup(html.text, 'html.parser')
        except:
            print('Erro ao conectar-se com o servidor')
            break


            #Conferindo se a página existe -> (no scrum n dá erro, ent precisa conferir):
        try:
            link_pag_atual = soup.find('li', 'pager__item is-active').find('a').get('href')
            if ('page='+str(page)) not in str(link_pag_atual):
                break

        except: #Se não existir 'pager__item is-active', só executa essa vez
            break_next = True


                        #Análise Scrum.org

        #Por poster encontrado na página:
        for poster in soup.select('.list-view-item'):
                #Recebemos o tipo    
            tipo_poster = poster.select_one('.list-view-item-type')
                #Tratamento
            if tipo_poster == None: #Caso o tipo não exista
                tipo_poster = ''
            else:
                tipo_poster = tipo_poster.text.strip()

                #Recebemos o título
            titulo_poster_tag = poster.select_one('.list-view-item-title')
            if titulo_poster_tag == None:
                titulo_poster = ''
            else:
                titulo_poster = titulo_poster_tag.text.strip()
            
            #Se o titulo já existir na database, para aqui
            if titulo_poster in all_titles:
                break_next = True
                break

            #Recebendo o href do titulo -> tranformar no link completo
            href = titulo_poster_tag.get('href')
            if href == None:
                link_poster = ''
            else:
                link_poster = 'https://www.scrum.org' + str(href)

            #Recebendo data
            data_poster = poster.select_one('.list-view-item-date')
            if data_poster == None:
                data_poster = ''
            else:
                data_poster = converterData(data_poster.text)

            #Recebendo a descrição    
            descricao_poster = poster.select_one('.list-view-item-teaser')
            if descricao_poster == None:
                descricao_poster = ''
            else:
                descricao_poster = descricao_poster.text.strip()
            

                #Salvando no dicionario do dataframe
            data['tipo'].append(tipo_poster)
            data['titulo'].append(titulo_poster)
            data['link'].append(link_poster)
            data['data'].append(data_poster)
            data['descricao'].append(descricao_poster)

        if break_next:
            break

    return data
    