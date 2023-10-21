import re
import sqlite3  # подключить библиотеку для работы с SQLite
import time
from urllib.parse import urlparse
from collections import Counter


from tqdm import tqdm

from utils import *


def get_popular_domain(db_file_name):
    db = db_file_name
    conn = sqlite3.connect(db)
    curs = conn.cursor()
    curs.execute('SELECT url FROM urllist')
    urls = [row[0] for row in curs.fetchall()]
    new_urls = []
    for url in urls:
        u = re.search('(://)(.*\..{1,3})(/?)', url)
        if u:
            new_urls.append(u[2])
        else:
            print(url)
    print(len(urls), len(new_urls))
    c = Counter(new_urls).most_common()
    for el in c[:20]:
        print(f'{el[1]} : {el[0]}')


class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, db_file_name):
        self.db = db_file_name
        self.conn = sqlite3.connect(self.db)
        self.curs = self.conn.cursor()
        log_file = open('logs.txt', 'w')
        log_file.close()
        print("Конструктор")

    # 0. Деструктор
    def __del__(self):
        self.curs.close()
        self.conn.close()
        print("Деструктор")

    def log_indexing(self):
        log_file = open('logs.txt', 'a')
        words_added = self.curs.execute("SELECT COUNT() FROM wordlist").fetchone()[0]
        word_locations_added = self.curs.execute("SELECT COUNT() FROM wordlocation").fetchone()[0]
        urls_added = self.curs.execute("SELECT COUNT() FROM urllist").fetchone()[0]
        log_file.write(f'{urls_added}  {words_added}  {word_locations_added}\n')
        log_file.close()

    def word_exists_or_insert(self, word, is_filtered):
        word = word.lower()
        word_exists = self.get_entry_id("wordlist", "word", word)
        if not word_exists:
            self.curs.execute("INSERT INTO wordlist(word, isFiltered) values(?, ?)", (word, is_filtered))
            self.conn.commit()
            return self.curs.execute("SELECT rowid FROM wordlist WHERE word=?", (word,)).fetchone()[0]
        return word_exists[0]

    def url_exists_or_insert(self, url):
        url_exists = self.get_entry_id("urllist", "url", url)
        if not url_exists:
            self.curs.execute("INSERT INTO urllist(url) values (?)", (url,))
            self.conn.commit()
            return self.curs.execute("SELECT rowid FROM urllist WHERE url=?", (url,)).fetchone()[0]
        return url_exists[0]

    # 1. Индексирование одной страницы
    def add_index(self, text, url):
        word_list = self.separate_words(text)
        if not self.is_indexed(url=url):
            for i, word in enumerate(tqdm(word_list)):
                word_id = self.word_exists_or_insert(word, 0)
                url_id = self.url_exists_or_insert(url)
                self.curs.execute("INSERT INTO wordlocation(fk_wordid, fk_URLId, location) values(?, ?, ?)",
                                  (word_id, url_id, i))
                self.conn.commit()

    # 3. Разбиение текста на слова (https://www.geeksforgeeks.org/python-spilt-a-sentence-into-list-of-words/)
    def separate_words(self, text):
        return re.findall(r'\b\w+\b', text)

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def is_indexed(self, url):
        exist_id = self.curs.execute("SELECT rowid FROM urllist WHERE url=?", (url,)).fetchone()
        if not exist_id:
            return False
        if not self.curs.execute("SELECT * FROM wordlocation WHERE fk_URLId=?", (exist_id[0],)).fetchmany(1):
            return False
        return True

    # 5. Добавление ссылки с одной страницы на другую
    def add_link_ref(self, url_from, url_to, link_text):
        url_from_id = self.url_exists_or_insert(url_from)
        url_to_id = self.url_exists_or_insert(url_to)
        self.curs.execute("INSERT INTO linkbetweenurl(fk_fromURL_id, fk_toURL_id) values (?, ?)",
                          (url_from_id, url_to_id))
        link_id = self.curs.execute("SELECT rowid FROM linkbetweenurl WHERE fk_fromURL_id=? AND fk_toURL_id=?",
                                    (url_from_id, url_to_id)).fetchone()[0]

        words_from_text = self.separate_words(link_text)
        for word in words_from_text:
            word_id = self.word_exists_or_insert(word, 0)
            self.curs.execute("INSERT INTO linkword(fk_word_id, fk_link_id) values (?, ?)",
                              (word_id, link_id))
        self.conn.commit()

    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, url_list, max_depth=1):
        print("Обход страниц")
        new_url_list = []
        for _curr_depth in range(0, max_depth):
            if new_url_list:
                url_list = new_url_list.copy()
            print(url_list)
            for url in url_list:
                try:
                    print(url)
                    soup = get_clean_html_from_url(url)

                    # print(soup.prettify())
                    found_links = soup.find_all('a', href=True)
                    print("Поиск ссылок")
                    time.sleep(0.005)
                    progress_bar = tqdm(found_links)
                    for link in progress_bar:
                        adding_link = link['href']
                        base_url = '://'.join(urlparse(url)[:2])
                        if link['href'].startswith('/'):
                            adding_link = base_url + adding_link
                        if adding_link not in new_url_list:
                            new_url_list.append(adding_link)
                        progress_bar.set_postfix_str(f'URL: {adding_link}')
                        self.add_link_ref(url, adding_link, link.text)
                    text = get_text_only(soup)
                    print("Индексация")
                    time.sleep(0.05)
                    self.add_index(text, url)
                    self.log_indexing()
                except Exception as e:
                    print(e)
                    print("  Не могу открыть %s" % soup.title)

    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def get_entry_id(self, table_name, field_name, value):
        query = """SELECT rowid FROM {table} WHERE {field}=?""".format(table=table_name, field=field_name, )
        return self.curs.execute(query, (value,)).fetchone()

    def init_db(self):
        # 7. Инициализация таблиц в БД
        # Таблица Word_List
        self.curs.execute("""DROP TABLE IF EXISTS wordlist""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS wordlist  (
                        rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ
                        word TEXT NOT NULL UNIQUE, -- слово
                        isFiltered INTEGER NOT NULL -- флаг фильтрации
                    ); """
                          )

        # Таблица URL_List
        self.curs.execute("""DROP TABLE IF EXISTS urllist""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS urllist  (
                        rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ
                        url TEXT NOT NULL UNIQUE -- ссылка
                     ); """
                          )

        # Таблица Word_Location
        self.curs.execute("""DROP TABLE IF EXISTS wordlocation""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS wordlocation(
                        rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ
                        fk_wordid INTEGER,                        -- ссылка на слово
                        fk_URLId INTEGER, -- ссылка на страницу
                        location INTEGER, -- позиция слова на странице
                        FOREIGN KEY (fk_wordid) REFERENCES wordlist (rowid),
                        FOREIGN KEY (fk_URLId) REFERENCES urllist (rowid)
                     ); """
                          )

        # Таблица Link_Between_URL
        self.curs.execute("""DROP TABLE IF EXISTS linkbetweenurl""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS linkbetweenurl(
                                rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ
                                fk_fromURL_id INTEGER,                        -- ссылка на страницу 1
                                fk_toURL_id INTEGER, -- ссылка на страницу 2
                                FOREIGN KEY (fk_fromURL_id) REFERENCES urllist (rowid),
                                FOREIGN KEY (fk_toURL_id) REFERENCES urllist (rowid)
                             ); """
                          )

        # Таблица Link_Word
        self.curs.execute("""DROP TABLE IF EXISTS linkword""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS linkword(
                                        rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ
                                        fk_word_id INTEGER,                        -- ссылка на слово
                                        fk_link_id INTEGER, -- ссылка на ссылку
                                        FOREIGN KEY (fk_word_id) REFERENCES  wordlist (rowid),
                                        FOREIGN KEY (fk_link_id) REFERENCES linkbetweenurl (rowid)
                                     ); """
                          )

        self.conn.commit()

        print("Создать пустые таблицы с необходимой структурой")

        # for link in found_links:
        # adding_link = link['href']
        # if link['href'].startswith('/'):
        #    adding_link = 'https://elementy.ru' + adding_link
        #    new_url_list.append('https://elementy.ru' + link['href'])
        # self.add_link_ref(url_from=url, url_to=adding_link, link_text=link.text)
