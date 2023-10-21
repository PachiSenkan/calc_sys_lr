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
    _searcher = Searcher(fileName)
    _searcher.calculate_page_rank(iterations=10)


#Инициализация БД
fileName = "DB_Indexation.db"

url_list = ['https://elementy.ru/novosti_nauki', 'https://nplus1.ru']


query = input('Введите запрос: ')
searcher = Searcher(fileName)
try:
    searcher.get_sorted_list(query, metric=pagerank_score)
except Exception as e:
    print(e)
