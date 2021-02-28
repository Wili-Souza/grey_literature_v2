from bs4 import BeautifulSoup
import requests
import pandas as pd

#  ---- Minhas funções e classes
from search import searchScrum
from exportar import exportar_df
from AC_scraper_mod.searching_mod import scr_search_page
from AC_scraper_mod.regular_mod import scr_page
from AC_scraper_mod.presentations_mod import scr_presentations_page

data = { # -> diciionário para data frame
    'tipo': [],
    'titulo': [],
    'link': [],
    'data': [],
    'descricao': [],
    'autor': []
}

selectors = {
    'title':'td.views-field.views-field-title',
    'autor':'div.field-name-user-row',
    'date':'div.field-post-date',
    'teaser':'td.views-field.views-field-title'
}

def agileConnection_scraper():
    print("Iniciando execução no Agile Connection ...")
    search_index = -1

        #Conectando com a página
    try:
        html = requests.get('https://www.agileconnection.com/')
        soup = BeautifulSoup(html.text, 'html.parser')
    except:
        print('Erro ao conectar-se com o servidor')

                    #Análise agileconnection.com

    #Raspando links de cada tipo de fontes (resources)
    for fonte in soup.find('div', {'id': 'tb-megamenu-column-2'}).find('ul').find_all('li'):
        url = 'https://www.agileconnection.com' + fonte.find('a').get('href')

            # --- Tratamento da string de busca
        search_index += 1

        #intervalo de pag da categoria:
        lastPage = 2

        pag = url  #primeira página será a página inicial

            # --- Conectando com a página da fonte (resources)
        try:
            html = requests.get(pag)
            soup = BeautifulSoup(html.text, 'html.parser')
        except:
            #print(f'Erro 1 ao conectar-se com {pag}')
            continue

            # --- Raspando as páginas
        if search_index == 2:
            scr_presentations_page(soup, lastPage, data)
            
        else:
            scr_page(soup, lastPage, selectors, data, search_index)

    return data
   