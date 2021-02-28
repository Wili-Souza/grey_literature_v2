#from bs4 import BeautifulSoup

def findType(soup):
    type_post = soup.select_one('div.aa-result-card__category').select_one('i').get('class')

    _type = type_post[0].replace('icon-', '').strip() #Será usado na formação de classes dinâmicas
    type_post = _type.replace('aa_', '').replace('_', ' ').title().strip()
    
    if type_post == 'Glossary':
        type_post += ' Terms'
    
    if type_post == None:
        type_post = ''

    return type_post, _type

    
