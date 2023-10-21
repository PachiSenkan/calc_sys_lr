import requests
from bs4 import BeautifulSoup

from crawler import *
from searcher import *
from utils import *


def crawl(_url_list, restart_db=False):
    web_crawler = Crawler(fileName)
    if restart_db:
        web_crawler.init_db()
    max_depth = 2                            
    web_crawler.crawl(_url_list, max_depth)


#Инициализация БД
fileName = "DB_Indexation.db"
url_list = ['https://elementy.ru/novosti_nauki', 'https://nplus1.ru']



#create_marked_html_file('index.html', 'https://elementy.ru/novosti_nauki', ['науки', 'новости'])
#get_popular_domain(fileName)
query = input('Введите запрос: ')
#query = 'Мамонт наука история'
searcher = Searcher(fileName)
#searcher.calculate_page_rank()
try:
    searcher.get_sorted_list(query, metric=pagerank_score)
except Exception as e:
    print(e)
#print(searcher.get_url_name(10))
#try:
#    searcher.get_words_ids('Новости науки')
#except Exception as e:
#    print(e)

#my_search_query = 'новости науки мамонт'
#rows_loc, words_id_list = searcher.get_match_rows(my_search_query)
#
#print("-----------------------")
#print(my_search_query)
#print(words_id_list)
#if not rows_loc:
#    print('Ничего')
#print(len(rows_loc))
#for location in rows_loc[:25]:
#    print(location)
