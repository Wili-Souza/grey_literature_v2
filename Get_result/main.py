from bs4 import BeautifulSoup
import requests
import pandas as pd
from copy import deepcopy
from time import sleep
from pymongo import MongoClient
from decouple import config

from exportar import exportar_df

# Conectando com o banco de dados em servidor
host = config("MONGO_HOST")
client = MongoClient(host)
db = client.greyDB

# coletando coleções
collectionAA = db.postsAA
collectionAC = db.postsAC
collectionS = db.postsS
all_collections = [*list(collectionS.find({})), *list(collectionAA.find({})), *list(collectionAC.find({}))]

all_titles = [x['titulo'] for x in all_collections ]
all_contents = [x['descricao'] for x in all_collections]

data = { # -> diciionário para data frame 
    'tipo': [],
    'titulo': [],
    'link': [],
    'data': [],
    'descricao': [],
    'autor': []
}

# tratamento_string recebe a string de busca e retorna combinações possíveis a partir dos operadores AND e OR
def tratamento_string(string_atual, opened=False, closed=False, continuing=False, list_all_searches = []):
    idx = 0
    this_string = ''

    if string_atual[idx] != '(':
        for i in range(0, len(string_atual)):
            if string_atual[i] == '(':
                opened, opened_idx = True, i
                break
            if string_atual[i] == ')':
                closed, closed_idx = True, i
                break

            this_string += string_atual[i]
        
        string_atual = string_atual[i:]

        while '*AND' in this_string:
            if this_string.strip().find('*AND') == 0:
                continuing = True

            and_idx = this_string.find('*AND')
            
            #tratando o que há antes do and 
            if '*OR' in this_string[:and_idx]:
                aux_string = this_string[:and_idx].split('*OR')

                for i in range(0, len(aux_string) - 1):
                    if aux_string[i].strip() != '':
                        list_all_searches.append('"{} --"' .format(aux_string[i].strip()))
            
                list_all_searches.append('"{}" ' .format(aux_string[-1].strip()))
                idx_waiting = len(list_all_searches) - 1

                #A parte antes do and já foi tratada:
                this_string = this_string[and_idx:]
                and_idx = 0 #novo and_idx
            else:
                if not continuing:
                    list_all_searches.append('"{}" ' .format(this_string[:and_idx].strip()))
                    idx_waiting = len(list_all_searches) - 1

            #Tratando o que há depois do and
            aux_string = this_string[and_idx+4:].strip()
            if '*OR' in aux_string or '*AND' in aux_string:
                or_idx = aux_string.find('*OR')
                and_idx = aux_string.find('*AND')
                idx_op = or_idx if (or_idx < and_idx and or_idx > -1) or and_idx < 0 else and_idx

                #tirar aspas
                if continuing:
                    try:
                        list_all_searches[idx_waiting] = list_all_searches[idx_waiting] + ' '
                    except: #Caso da função recursiva com combinações
                        for i in range(0, len(list_all_searches)):
                            if 'waiting' in list_all_searches[i]:
                                list_all_searches[i] = list_all_searches[i] + ' '

                try:
                    list_all_searches[idx_waiting] += '"{}"' .format(aux_string[:idx_op].strip())
                except:
                    for i in range(0, len(list_all_searches)):
                            if 'waiting' in list_all_searches[i]:
                                list_all_searches[i] += '"{}"' .format(aux_string[:idx_op].strip())
            else:
                idx_op = None
                if continuing:
                    try:
                        list_all_searches[idx_waiting] = list_all_searches[idx_waiting][:-1] + ' '
                        list_all_searches[idx_waiting] += '{}"' .format(aux_string.strip())

                    except: #Caso da função recursiva com combinações
                        for i in range(0, len(list_all_searches)):
                            if 'waiting' in list_all_searches[i]:
                                if aux_string.strip() != '':
                                    list_all_searches[i] = list_all_searches[i] + ' "' + aux_string.strip() + '"'
                
                else:
                    try:
                        if aux_string.strip() != '':
                            list_all_searches[idx_waiting] += '"{}"' .format(aux_string.strip())

                    except: #Caso da função recursiva com combinações - need testing
                        for i in range(0, len(list_all_searches)):
                            if 'waiting' in list_all_searches[i]:
                                list_all_searches[i] = list_all_searches[i][:-1] + ' ' + aux_string.strip()
            
            if idx_op:
                this_string = aux_string[idx_op:].strip()
            else:
                break

        if '*OR' in this_string:
            if '*AND' in string_atual[0:4].strip():
                    for item in this_string.split('*OR'):
                        if item.strip() != '':
                            list_all_searches.append('"{}" waiting"' .format(item.strip()) )

            else:
                for item in this_string.split('*OR'):
                    if item.strip() != '':
                        list_all_searches.append('"{}"' .format(item.strip()) )
        
        if opened and not closed:
            if '*AND' in this_string[-5:]:
                continuing = True

            if continuing:
                for k in range(len(list_all_searches) - 1, -1, -1):
                    if '--' in list_all_searches[k]:
                        break
                    else:
                        list_all_searches[k] = list_all_searches[k].replace('waiting','') +'waiting'

            list_all_searches = tratamento_string(
                string_atual, 
                opened=True, 
                continuing=continuing,
                list_all_searches=list_all_searches
                )

        return list_all_searches
    
    else:
        for i in range(0, len(string_atual)):
            if string_atual[i] == '(':
                if i != 0:
                    opened, opened_idx = True, i
                    break
                else:
                    continue

            if string_atual[i] == ')':
                closed, closed_idx = True, i
                break
        
            this_string += string_atual[i]
        
        string_atual = string_atual[i+1:]

        if continuing:
            new_items = []
            if '*OR' in this_string.strip() or '*AND' in this_string.strip():
                parenthesis = tratamento_string(this_string, list_all_searches=list_all_searches) ##
            else:
                parenthesis = [*list_all_searches, f'"{this_string}"']

            for i in range(0, len(list_all_searches)):
                if '--' in list_all_searches[i]:
                    new_items.append(list_all_searches[i].replace(' --', ''))
                
                elif 'waiting' in list_all_searches[i]:

                    for j in range(i, len(parenthesis)):
                        if 'waiting' not in parenthesis[j]:
                            new_items.append(f'{list_all_searches[i]} {parenthesis[j]}')
            
            list_all_searches = new_items

            if string_atual.strip() != '':
                if '*AND' not in string_atual[0:5].strip():
                    for i in range(0, len(list_all_searches)):
                        list_all_searches[i] = list_all_searches[i].replace('waiting', '')

                list_all_searches = tratamento_string(
                    string_atual,  
                    list_all_searches=list_all_searches
                    )

        else:
            if '*OR' in this_string.strip() or '*AND' in this_string.strip():
                aux = [*list_all_searches] #estava pegando o endereço na memória
                parenthesis = tratamento_string(this_string, list_all_searches=list_all_searches)

                parenthesis = list(set(parenthesis).difference(aux))
                list_all_searches = list(set(list_all_searches).difference(parenthesis))
            else:
                parenthesis = [f'"{this_string}"']

            for i in range(0, len(parenthesis)):
                list_all_searches.append(f'{parenthesis[i][:-1]} waiting"')

            if string_atual.strip() != '':
                if '*AND' not in string_atual[0:5].strip():
                    for i in range(0, len(list_all_searches)):
                        list_all_searches[i] = list_all_searches[i].replace('waiting', '')

                list_all_searches = tratamento_string(
                    string_atual,  
                    list_all_searches=list_all_searches
                    )

    return list_all_searches 

# marca os operadores AND and OR para serem identificados 
def mark_operators(input_string):
    quotes_counter = 0
    waiting_other = False
    for i in range(0, len(input_string)):
        if input_string[i] == '"':
            waiting_other = False
            quotes_counter += 1
        
        if quotes_counter % 2 == 0 and not waiting_other:
            if input_string[i+2:i+4] == 'OR' or input_string[i+2:i+5] == 'AND':
                input_string = input_string[:i+2] + "*" + input_string[i+2:]
                i = i+4
                waiting_other = True

            elif input_string[i+3:i+5] == 'OR' or input_string[i+3:i+6] == 'AND': 
                input_string = input_string[:i+3] + "*" + input_string[i+3:]
                i = i + 5
                waiting_other = True

    return input_string.replace('"', '')

# Retira marcadores remanescentes
def clean_result(result):
    for i in range(0, len(result)):
        result[i] = result[i].replace('"waiting"', '').replace(' "waiting', '"').replace('waiting', '').replace('--', '').strip()

        result[i] = " ".join(result[i].split())
    
    return result

# Faz a busca das combinações nas bases do banco de dados
def search_filter():
    #---------  Fazendo scraping com base nas combinações encontradas
    for j in range(len(all_collections) -1, -1, -1): 
        print(j)
        validated = False
        content = ''

        #Checando se o post possui alguma das combinações requeridas
        for item in result:
            valid_post = True
            item_elements = [x.strip() for x in item.split('"') if x.strip() != '']

            for i in range(0, len(item_elements)):
                if item_elements[i].lower() not in all_collections[j]['titulo'].lower() and \
                    item_elements[i].lower() not in all_collections[j]['descricao'].lower():
                    valid_post = False
                    break

            if valid_post:
                data['tipo'].append(all_collections[j]['tipo'])
                data['titulo'].append(all_collections[j]['titulo'])
                data['link'].append(all_collections[j]['link'])
                data['data'].append(all_collections[j]['data'])
                data['descricao'].append(all_collections[j]['descricao'])
                data['autor'].append(all_collections[j]['autor'])
                break

    print("{} resultados encontrados." .format(len(data['link'])))

# --- EXECUTANDO 
search_string = input("String: ") # ex.: ("Termo 1" OR "Termo 2)" AND "Termo 3"
result = tratamento_string(mark_operators(search_string))
result = clean_result(result)

# filtrando dados do banco de dados a partir dos resultados
search_filter()

# Convertendo o dicionário para data frame
df = pd.DataFrame(data, columns = ['tipo', 'titulo', 'link', 'data', 'autor', 'descricao'])

# Exportando o dataframe como xlsx (excel) e csv -> é salvo em ./resultados
exportar_df(df, "xlsx", "csv", wb="T")
