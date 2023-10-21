import glob
import os
import re
import requests
from bs4 import BeautifulSoup


def get_clean_html_from_url(url):
    html_doc = requests.get(url).text
    soup = BeautifulSoup(html_doc, "html.parser")
    list_unwanted_items = ['script', 'style']
    for script in soup.find_all(list_unwanted_items):
        script.decompose()
    dec = soup.find('div', {'class': 'pager'})
    if dec:
        dec.decompose()
    dec = soup.find('div', {'class': 'hfooter'})
    if dec:
        dec.decompose()
    dec = soup.find('div', {'class': 'menuarea'})
    if dec:
        dec.decompose()
    dec = soup.find('div', {'class': 'hide_print'})
    if dec:
        dec.decompose()
    return soup


def get_text_only(soup):
    text = soup.getText()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    found_img = soup.find_all('img')
    img_text = '\n'.join(img['alt'] for img in found_img if 'alt' in img.attrs)
    text = '\n'.join([text, img_text])
    return text


def get_marked_html(word_list, query_list, info_string=''):
    """Генерировть html-код с маркировкой указанных слов цветом
    :param wordList - список отдельных слов исходного текста
    :param queryList - список отдельных искомых слов,
    """
    color_dict = {}
    color_list = ['SandyBrown', 'SkyBlue', 'Gold', 'LightPink', 'Orchid']
    if len(query_list) <= len(color_list):
        for i, q_word in enumerate(query_list):
            color_dict[q_word] = color_list[i]
    result_html = f'<h2>{info_string}</h1>'
    result_html += '<p>'
    for word in word_list:
        if word in query_list:
            result_html += f' <span style="background-color:{color_dict[word]}">{word}</span> '
        else:
            result_html += f' {word} '
    result_html += ' </p>'
    return result_html


def create_marked_html_file(marked_html_filename, url, query, info_string=''):
    query = query.lower()
    query = query.split()

    # Прeобразование текста к нижнему регистру
    test_text = get_text_only(get_clean_html_from_url(url))
    test_text = test_text.lower()
    for i in range(0, len(query)):
        query[i] = query[i].lower()

    # Получения текста страницы с знаками переноса строк и препинания. Прием с использованием регулярных выражений
    word_list = re.compile("[\\w]+|[\\n.,!?:—]").findall(test_text)

    # Получить html-код с маркировкой искомых слов
    html_code = get_marked_html(word_list, query, info_string)

    # сохранить html-код в файл с указанным именем
    file = open(marked_html_filename, 'w', encoding="utf-8")
    file.write(html_code)
    file.close()
