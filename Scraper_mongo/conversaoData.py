from datetime import datetime

def converterData(data):
    #intervalo = intervalo.split('-')
    data = data.strip()

    '''if 'January' in data:
        data = data.replace('January', '01')
    elif 'February' in data:
        data = data.replace('February', '02')
    elif 'March' in data:
        data = data.replace('March', '03')
    elif 'April' in data:
        data = data.replace('April', '04')
    elif 'May' in data:
        data = data.replace('May', '05')
    elif 'June' in data:
        data = data.replace('June', '06')
    elif 'July' in data:
        data = data.replace('July', '07')
    elif 'August' in data:
        data = data.replace('August', '08')
    elif 'September' in data:
        data = data.replace('September', '09')
    elif 'October' in data:
        data = data.replace('October', '10')
    elif 'November' in data:
        data = data.replace('November', '11')
    elif 'December' in data:
        data = data.replace('December', '12')'''
    
    try:
        data = datetime.strptime(data, '%B %d, %Y')
        data = data.strftime('%d/%m/%Y')

    except ValueError:
        items_data = data.split('/')
        for i in range(0, len(items_data)):
            if len(items_data[i]) == 1:
                items_data[i] = '0' + items_data[i]
                
        data = f'{items_data[1]}/{items_data[0]}/20{items_data[2]} '

    #data = datetime.strptime(data, '%m %d, %Y')
    
    """ 
    if len(intervalo) > 2 or len(intervalo) < 1:
        print("Warning: Intervalo de data mal formatado, não será aplicado.")
    else:
        if len(intervalo) == 2:
            minimo = (int(x) for x in intervalo[0].strip().split('/'))
            maximo = (int(x) for x in intervalo[1].strip().split('/'))
            data_artigo = (int(x) for x in data.strip().split('/'))

            if (data_artigo[0] >= minimo[0] and data_artigo[1] >= minimo[1] and data_artigo[2] >= minimo[2]) \
            and (data_artigo[0] <= maximo[0] and data_artigo[1] <= maximo[1] and data_artigo[4] <= maximo[3]):
                filtro = True
            else:
                filtro = False

        if len(intervalo) == 1:
            minimo = (int(x) for x in intervalo[0].strip().split('/'))
            data_artigo = (int(x) for x in data.strip().split('/'))

            if (data_artigo[0] >= minimo[0] and data_artigo[1] >= minimo[1] and data_artigo[2] >= minimo[2]):
                filtro = True
            else:
                filtro = False
    """
    return data #, filtro

''' # --------------- Teste

novaData = converterData('May 12, 2020')
print(novaData)
print(type(novaData))'''