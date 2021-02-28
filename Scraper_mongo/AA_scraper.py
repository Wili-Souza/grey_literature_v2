from selenium import webdriver   # Site dinamico -> Selenium
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
from datetime import datetime
from server.connection import db

from AA_scraper_mod.postType import findType
from pymongo import MongoClient

# client = MongoClient('mongodb://localhost:27017/')
# db = client.greyDB
collection = db.postsAA
all_links = [x['link'] for x in list(collection.find({}))]

#  ---- Minhas funções e classes
from AA_scraper_mod.scraper import scrap

data = { # -> diciionário para data frame
    'tipo': [],
    'titulo': [],
    'link': [],
    'data': [],
    'descricao': [],
    'autor': []
}

def agileAlliance_scraper():
    print("Iniciando execução no Agile Alliance ...")

    num_pag_scraped = 0
    finished = False
    lastPage = 9999

    # --------------- Conectando com o Selenium
    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_argument("--headless")
    chromeOptions.add_argument('--disable-gpu')
    chromeOptions.add_argument("--no-sandbox")
    chromeOptions.add_argument("--disable-dev-shm-usage")
    chromeOptions.add_argument("--window-size=1920x1080")
    chromeOptions.add_argument("start-maximised")

    driver = webdriver.Chrome('chromedriver.exe', options=chromeOptions)

    #Conectando com a página usando o driver
    driver.get('https://www.agilealliance.org/resources')
    sleep(5) #Tempo de carregamento da página

    try:
        pagination_div = driver.find_element_by_class_name('aa-search-pagination')
        test = driver.find_elements_by_class_name('aa-search-pagination__btn')[1]
    except:
        pagination_div = None

    # --------------- conexão BeautifulSoup através do driver
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
    except:
        print('Erro ao conectar-se com o servidor')
        finished = True


    last_height = driver.execute_script("return document.body.scrollHeight")

    # ------- Carregando os posts
    if pagination_div is None:
        while not finished:
            num_pag_scraped += 1
            
            #intervalo de páginas
            if num_pag_scraped == lastPage:
                break

            # Checando existentes ---------------

            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
            except:
                print('Erro ao conectar-se com a nova página.')
                return -1

            # ---------- Scraping dos posts do Agile Alliance carregados
            posts = soup.select_one('ul.aa-search__results').find_all('li', 'wrap') #Pegando todos os posts

            for post in posts:
                    #Recebendo o href (faz uso do type, extraido em postType.py)
                tipo_post, _type = findType(post)
                try:
                    href = post.select_one(f'a.aa-result-card.aa-result-card--{_type}').get('href')
                    link_post = str(href)
                except AttributeError:
                    link_post = ''
                
                # ----------------- filtro update 
                if link_post in all_links:
                    finished = True
                    break
                # --------------------------------

            if finished:
                break

            #Scroll down 
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight-1500);")
            #Espera carregar
            sleep(5)
            #Pega o novo comprimento da página
            new_height = driver.execute_script("return document.body.scrollHeight")

            seconds = 0
            while new_height == last_height: #Se for a mesma -> tenta novamente
                sleep(2)
                seconds += 2
                new_height = driver.execute_script("return document.body.scrollHeight")

                if seconds > 60*15: #Para pegar uma grande quantidade de páginas, alterar para 60, por causa da lentidão
                    finished = True
                    break


            last_height = new_height #Atualiza o último comprimento
            
        scrap(driver, data) #Faz o web-scraping

    else:
        while not finished and num_pag_scraped != lastPage:
            num_pag_scraped += 1
            scrap(driver, data) #Faz o web-scraping

            #Passando a página estática
            next_pag_btn = driver.find_elements_by_class_name('aa-search-pagination__btn')[1] #div do botão next
            if next_pag_btn.get_attribute('disabled'):
                finished = True
            else:
                next_pag = next_pag_btn.find_element_by_class_name('fa-angle-right')
                driver.execute_script("arguments[0].click();", next_pag)
                sleep(3)


    #Encerra o driver
    driver.quit()

    return data
   