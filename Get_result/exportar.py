#-> SALVANDO EM XLSX E CSV com tratamento em caso de já existir
import pandas as pd
import xlsxwriter

# --- Meus módulos
from formatar import formatar_excel

def exportar_df(df, *args, **kwargs):
    ne = 0
    nc = 0
    website = 0

    if kwargs['wb'] == 'S':
        website = 0
    elif kwargs['wb'] == 'AC':
        website = 1
    elif kwargs['wb'] == 'AA':
        website = 1
    elif kwargs['wb'] == 'T':
        website = 2

    if 'xlsx' not in args and 'csv' not in args:
        return -1, -1
    
    if 'xlsx' in args:
        try:
            while(True): #-> tenta abrir até encontrar um que n exista
                try:
                    with open('resultados/resultado_excel_{}.xlsx' .format(str(ne)), 'r'): 
                        ne += 1
                except PermissionError:
                    continue

        except FileNotFoundError:
            writer = pd.ExcelWriter('resultados/resultado_excel_{}.xlsx' .format(str(ne)), engine='xlsxwriter') 
            df.to_excel(writer, 'Sheet1') #convertendo

            writer = formatar_excel(writer, website)

            #salvando alterações e criando arquivo
            writer.save()

    if 'csv' in args:
        try:
            while(True): #-> tenta abrir até encontrar um que n exista
                with open('resultados/resultado_csv_{}.csv' .format(str(nc)), 'r'): 
                    nc += 1
                   
        except FileNotFoundError:
            df.to_csv('resultados/resultado_csv_{}.csv' .format(str(nc)), encoding='UTF-8')

    return ne, nc
