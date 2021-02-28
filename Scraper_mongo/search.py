from bs4 import BeautifulSoup
import requests

def searchScrum(s_name, s_tag, s_type):
    html2 = requests.get('https://www.scrum.org/resources')
    soup2 = BeautifulSoup(html2.text, 'html.parser')

    tag_options = soup2.find(id='edit-field-resource-tags-target-id').find_all('option')
    type_options = soup2.find(id='edit-type').find_all('option')

    search_str = ''

    if s_name.strip() != '':
        search_str = f'search={s_name.strip()}&'

    for option in tag_options:
        if option.text.lower() == s_tag.lower().strip():
            search_str += 'field_resource_tags_target_id={}&' .format(str(option.get('value')))
            break
        
    for option in type_options:
        if option.text.lower() == s_type.lower().strip():
            search_str += 'type={}&' .format(str(option.get('value')))
            break
    
    return search_str
