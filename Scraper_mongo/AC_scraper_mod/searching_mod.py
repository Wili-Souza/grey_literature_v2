from conversaoData import converterData
import requests
from bs4 import BeautifulSoup

from AC_scraper_mod.types import type_by_index

def scr_search_page(soup, lastPage, data, search_index):
    page_exist = True
    num_pag = 0
    tipo_post = type_by_index(search_index)

    while page_exist:

        if search_index != 4:
            try:
                first_post = [soup.select_one('div.view-content').select_one('div')]
                list_posts = first_post + soup.select_one('div.view-content').select_one('div').find_next_siblings('div')
            
            except:
                break
        else:
            try:
                first_post = [soup.select_one('div.view-content').select_one('tbody').select_one('tr')]
                list_posts = first_post + soup.select_one('div.view-content').select_one('tbody').select_one('tr').find_next_siblings('tr')
            
            except:
                break

        #Por poster encontrado na página:
        for post in list_posts:

            #Recebendo o título
            if search_index != 4:
                titulo_post_tag = post.select_one('h3.title').select_one('a')
                if titulo_post_tag == None:
                    titulo_post = ''
                else:
                    titulo_post = titulo_post_tag.text.strip()

                #Recebendo autor
                try:
                    autor_post = post.select_one('div.field.field-name-author.field-type-ds.field-label-hidden').select_one('a')
                except AttributeError: 
                    #Em presentations tem estrutura diferenciada
                    autor_post = post.select_one('div.field.field-name-field-author.field-type-text.field-label-hidden').select_one('div.field-item.even')

                if autor_post == None:
                    autor_post = ''
                else:
                    autor_post = autor_post.text.strip()
            
                #Recebendo a descrição    
                descricao_post = post.select_one('div.field.field-name-body.field-type-text-with-summary.field-label-hidden').select_one('p')
                if descricao_post == None:
                    descricao_post = ''
                else:
                    descricao_post = descricao_post.text.strip()
                
            else:
                #recebendo titulo e autor
                titulo_post_tag = post.select_one('h3').select_one('a')
                list_titulo_post_tag = titulo_post_tag.text.split('|')

                if len(list_titulo_post_tag) == 2:
                        titulo_post, autor_post = list_titulo_post_tag
                else:
                    titulo_post, autor_post = [list_titulo_post_tag[0].strip(), ' ']

                #Recebendo descrição
                descricao_post = post.select_one('h3').next_sibling

            #Recebendo o href do titulo -> tranformar no link completo
            href = titulo_post_tag.get('href')
            if href == None:
                link_post = ''
            else:
                link_post = str(href)
                if 'https://' not in link_post and 'http://' not in link_post:
                    if 'https/' not in link_post:
                        link_post = 'https://www.agileconnection.com' + link_post
                    else:
                        link_post.replace("https/", "https://")
            

            #Recebendo data
            data_post = post.select_one('div.field.field-name-post-date')
            if data_post == None:
                data_post = ''
            else:
                data_post = converterData(data_post.text.replace('-', ''))


                #Salvando no dicionario do dataframe
            data['tipo'].append(tipo_post)
            data['titulo'].append(titulo_post)
            data['link'].append(link_post)
            data['autor'].append(autor_post)
            data['data'].append(data_post)
            data['descricao'].append(descricao_post)

            #Checando se existe uma próxima página
        num_pag += 1
        if num_pag >= lastPage:
            break

        try:
            pag = 'https://www.agileconnection.com' + soup.select_one('li.pager-next').find('a').get('href')
        except:
            page_exist = False
            #print('Próxima página não encontrada.')
            break
        
            #Conectando com a próxima página
        try:
            html = requests.get(pag)
            soup = BeautifulSoup(html.text, 'html.parser')
        except:
            #print(f'Erro 2 ao conectar-se com {pag}')
            break