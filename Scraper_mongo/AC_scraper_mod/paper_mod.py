from conversaoData import converterData
import requests
from bs4 import BeautifulSoup

def scr_paper_page(soup, lastPage, data):
    page_exist = True

    while page_exist:

        #Por poster encontrado na página:
        num_posts = 0
        print("Executando em Agile Connection...")
        for post in soup.select_one('div', {'id': 'content'}).find_all('tr'):

            #Recebendo o título
            try:
                titulo_post_tag = post.select_one('td.views-field.views-field-title').select_one('a')
                titulo_post = titulo_post_tag.text.strip()
            except:
                break

            titulo_post, autor_post = titulo_post.split('|')

            #Recebendo o href do titulo -> tranformar no link completo
            href = titulo_post_tag.get('href')
            if href == None:
                link_post = ''
            else:
                link_post = 'https://www.agileconnection.com' + str(href)
            
            #Recebendo autor
            if autor_post == None:
                autor_post = ''
            else:
                autor_post = autor_post.strip()

            #Recebendo data
            data_post = post.select_one('div.field-post-date')
            if data_post == None:
                data_post = ''
            else:
                data_post = converterData(data_post.text)

            #Recebendo a descrição    
            descricao_post = post.select_one('td.views-field.views-field-title').select_one('p')
            if descricao_post == None:
                descricao_post = ''
            else:
                descricao_post = descricao_post.text.strip()
            
            tipo_poster = 'White Paper'

                #Salvando no dicionario do dataframe
            data['tipo'].append(tipo_poster)
            data['titulo'].append(titulo_post)
            data['link'].append(link_post)
            data['autor'].append(autor_post)
            data['data'].append(data_post)
            data['descricao'].append(descricao_post)

            num_posts += 1
            if num_posts >= 10:
                break

        
        try:
            pag = 'https://www.agileconnection.com' + soup.select_one('li.pager-next').find('a').get('href')
        except:
            page_exist = False
            break
        
            #Conectando com a próxima página
        try:
            html = requests.get(pag)
            soup = BeautifulSoup(html.text, 'html.parser')
        except:
            #print(f'Erro ao conectar-se com {pag}')
            break
        