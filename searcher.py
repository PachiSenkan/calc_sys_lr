import re
import sqlite3
import time
from collections import Counter

from tqdm import tqdm
from utils import *


def normalize_scores(scores, small_is_better=False):
    result_dict = dict()  # словарь с результатом

    v_small = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
    min_score = min(scores.values())  # получить минимум
    max_score = max(scores.values())  # получить максимум
    # перебор каждой пары ключ значение
    for (key, val) in scores.items():

        if small_is_better:
            # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
            # ранг нормализованный = мин. / (тек.значение  или малую величину)
            result_dict[key] = float(min_score) / max(v_small, val)
        else:
            # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
            # вычисление ранга как доли от макс.
            # ранг нормализованный = тек. значения / макс.
            result_dict[key] = float(val) / max_score

    return result_dict


def frequency_score(rows):
    counts = dict([(row[0], 0) for row in rows])
    for row in rows:
        counts[row[0]] += 1
    return normalize_scores(counts)


def distance_score(rows):
    # Если есть только одно слово, любой документ выигрывает!
    if len(rows[0]) <= 2:
        return dict([(row[0], 1.0) for row in rows])

    # Инициализировать словарь большими значениями
    min_distance = dict([(row[0], 1000000) for row in rows])
    for row in rows:
        dist = sum([abs(row[i] - row[i - 1]) for i in range(2, len(row))])
        if dist < min_distance[row[0]]:
            min_distance[row[0]] = dist
    return normalize_scores(min_distance, small_is_better=True)


def pagerank_score(rows, con):
    page_ranks = dict([(row[0],
                        con.execute(
                            'SELECT score FROM pagerank WHERE urlid= %d' % row[0]).fetchone()[0]) for row in rows])
    max_rank = max(page_ranks.values())
    normalized_scores = dict([(u, float(l) / max_rank) for (u, l) in page_ranks.items()])
    return normalized_scores


class Searcher:

    def db_commit(self):
        """ Зафиксировать изменения в БД """
        self.con.commit()

    def __init__(self, db_file_name):
        """  0. Конструктор """
        # открыть "соединение" получить объект "сonnection" для работы с БД
        self.con = sqlite3.connect(db_file_name)
        file_list = glob.glob('./*.html')
        for filePath in file_list:
            try:
                os.remove(filePath)
            except:
                print('Не удалился файл: ', filePath)

    def __del__(self):
        """ 0. Деструктор  """
        # закрыть соединение с БД
        self.con.close()

    def get_url_name(self, url_id):
        """
        Получает из БД текстовое поле url-адреса по указанному urlid
        :param url_id: целочисленный urlid
        :return: строка с соответствующим url
        """

        return self.con.execute(
            "select url from urllist where rowid=%d" % url_id).fetchone()[0]

    def get_sorted_list(self, query_string, metric=pagerank_score):
        """
        На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке
        :param metric: название функции метрики (frequency_score, location_score, distance_score, pagerank_score)
        :param query_string:  поисковый запрос
        :return:
        """
        rows_loc, word_ids = self.get_match_rows(query_string)
        if not rows_loc:
            raise Exception('Нет результата')
        print(len(rows_loc))
        print(len(list(Counter(row[0] for row in rows_loc).most_common())))
        if metric != pagerank_score:
            m1_scores = metric(rows_loc)
        else:
            m1_scores = metric(rows_loc, self.con)
        # получить rowsLoc и wordids от getMatchRows(queryString)
        # rowsLoc - Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..."
        # wordids - Список wordids.rowid слов поискового запроса

        # Получить m1Scores - словарь {id URL страниц где встретились искомые слова: вычисленный нормализованный РАНГ}
        # как результат вычисления одной из метрик

        # Создать список для последующей сортировки рангов и url-адресов
        ranked_scores_list = list()
        for url, score in m1_scores.items():
            pair = (score, url)
            ranked_scores_list.append(pair)

        # Сортировка из словаря по убыванию
        ranked_scores_list.sort(reverse=True)

        # Вывод первых N Результатов
        print('  #  score  urlid   url_name')
        for i, (score, url_id) in enumerate(ranked_scores_list):
            url_name = self.get_url_name(url_id)
            print("{:>3}  {:.3f} {:>6}   {}".format(i, score, url_id, url_name))
            if i < 10:
                filename = f'index_{i+1}.html'
                info_string = f'URL: {url_name}<br>Метрика: {metric.__name__}<br>Значение метрики: {score}'
                create_marked_html_file(filename, url_name, query_string, info_string)

    def get_words_ids(self, query_string):
        """
        Получение идентификаторов для каждого слова в queryString
        :param query_string: поисковый запрос пользователя
        :return: список wordlist.rowid искомых слов
        """

        # Привести поисковый запрос к нижнему регистру
        query_string = query_string.lower()

        # Разделить на отдельные искомые слова
        query_words_list = query_string.split(' ')

        # список для хранения результата
        rowid_list = list()

        # Для каждого искомого слова
        for word in query_words_list:
            # Сформировать sql-запрос для получения rowid слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            sql = f'SELECT rowid FROM wordlist WHERE word = "{word}" LIMIT 1;'
            # sql = "SELECT rowid FROM wordlist WHERE word =\"{}\" LIMIT 1; ".format(word)

            # Выполнить sql-запрос. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowid
            result_row = self.con.execute(sql).fetchone()

            # Если слово было найдено и rowid получен
            if result_row is not None:
                # Искомое rowid является элементом строки ответа от БД (особенность получаемого результата)
                word_rowid = result_row[0]

                # поместить rowid в список результата
                rowid_list.append(word_rowid)
                print(' ', word, word_rowid)
            else:
                # в случае, если слово не найдено приостановить работу (генерация исключения)
                raise Exception('Одно из слов поискового запроса не найдено:' + word)

        # вернуть список идентификаторов
        return rowid_list

    def get_match_rows(self, query_string):
        """
        Поиск комбинаций из всезх искомых слов в проиндексированных url-адресах
        :param query_string: поисковый запрос пользователя
        :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
        """

        # Разбить поисковый запрос на слова по пробелам
        query_string = query_string.lower()
        words_list = query_string.split(' ')

        # получить идентификаторы искомых слов
        words_id_list = self.get_words_ids(query_string)

        # Созать переменную для полного SQL-запроса
        sql_full_query = """"""

        # Созать объекты-списки для дополнений SQL-запроса
        sql_part_name = list()  # имена столбцов
        sql_part_join = list()  # INNER JOIN
        sql_part_condition = list()  # условия WHERE

        # Конструктор SQL-запроса (заполнение обязательной и дополнительных частей)
        # обход в цикле каждого искомого слова и добавлене в SQL-запрос соответствующих частей
        # for word_index in range(0, len(words_list)):
        for word_index, word_id in enumerate(words_id_list):

            # Получить идентификатор слова
            # word_id = words_id_list[word_index]

            if word_index == 0:
                # обязательная часть для первого слова
                sql_part_name.append("""w0.fk_URLId""")
                sql_part_name.append(""", w0.location""")

                sql_part_condition.append(f'WHERE w0.fk_wordid={word_id}')
                # ""WHERE w0.fk_wordid={}""".format(word_id))

            elif len(words_list) >= 2:
                # Дополнительная часть для 2,3,.. искомых слов

                # Проверка, если текущее слово - второе и более

                # Добавить в имена столбцов
                sql_part_name.append(f', w{word_index}.location')
                # """ , w{}.location w{}_loc --положение следующего искомого слова""".format(word_index,
                #                                                                          word_index))

                # Добавить в sql INNER JOIN
                sql_part_join.append(f'INNER JOIN wordlocation w{word_index}'
                                     f' on w0.fk_URLId=w{word_index}.fk_URLId')
                # """INNER JOIN wordlocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                #        on w0.fk_URLId=w{}.fk_URLId -- условие объединения""".format(word_index, word_index,
                #                                                                     word_index))
                # Добавить в sql ограничивающее условие
                sql_part_condition.append(f'AND w{word_index}.fk_wordid={word_id}')
                # """  AND w{}.fk_wordid={} -- совпадение w{}... с cоответсвующим словом """.format(word_index,
                #                                                                                  word_id,
                #                                                                                 word_index))

            # Объеднение запроса из отдельных частей

            # Команда SELECT
        sql_full_query += 'SELECT '

        # Все имена столбцов для вывода
        for sql_part in sql_part_name:
            sql_full_query += '\n'
            sql_full_query += sql_part

        # обязательная часть таблица-источник
        sql_full_query += '\n'
        sql_full_query += 'FROM wordlocation w0 '

        # часть для объединения таблицы INNER JOIN
        for sql_part in sql_part_join:
            sql_full_query += '\n'
            sql_full_query += sql_part

        # обязательная часть и дополнения для блока WHERE
        for sql_part in sql_part_condition:
            sql_full_query += '\n'
            sql_full_query += sql_part

        # Выполнить SQL-запроса и извлеч ответ от БД
        # print(sql_full_query)
        cur = self.con.execute(sql_full_query)
        rows = [row for row in cur]

        return rows, words_id_list

    def calculate_page_rank(self, iterations=5):
        self.set_up_db()
        for i in range(iterations):
            print(f'Итерация {i}')
            time.sleep(0.005)
            url_list = self.con.execute('select rowid from urllist').fetchall()
            progress_bar = tqdm(url_list, ncols=60)
            for (url_id,) in progress_bar:
                pr = 0.15
                # В цикле обходим все страницы, ссылающиеся на данную
                for (linker,) in self.con.execute(
                        'SELECT DISTINCT fk_fromURL_id FROM linkbetweenurl WHERE fk_toURL_id=%d' % url_id):
                    # Находим ранг ссылающейся страницы
                    linking_pr = self.con.execute(
                        'SELECT score FROM pagerank WHERE urlid=%d' % linker).fetchone()[0]
                    # Находим общее число ссылок на ссылающейся странице
                    linking_count = self.con.execute(
                        'SELECT COUNT(*) FROM linkbetweenurl WHERE fk_fromURL_id=%d' % linker).fetchone()[0]
                    pr += 0.85 * (linking_pr / linking_count)
                self.con.execute(
                    'UPDATE pagerank SET score=%f WHERE urlid=%d' % (pr, url_id))
                self.db_commit()

    def set_up_db(self):
        # Подготовка БД ------------------------------------------
        # стираем текущее содержимое таблицы PageRank
        self.con.execute('DROP TABLE IF EXISTS pagerank')
        self.con.execute("""CREATE TABLE  IF NOT EXISTS  pagerank(
                                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                    urlid INTEGER,
                                    score REAL
                                );""")

        # Для некоторых столбцов в таблицах БД укажем команду создания объекта "INDEX" для ускорения поиска в БД
        self.con.execute("DROP INDEX   IF EXISTS wordidx;")
        self.con.execute("DROP INDEX   IF EXISTS urlidx;")
        self.con.execute("DROP INDEX   IF EXISTS wordurlidx;")
        self.con.execute("DROP INDEX   IF EXISTS urltoidx;")
        self.con.execute("DROP INDEX   IF EXISTS urlfromidx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS wordidx       ON wordlist(word)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlidx        ON urllist(url)')
        self.con.execute('CREATE INDEX IF NOT EXISTS wordurlidx    ON wordlocation(fk_wordid)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urltoidx      ON linkbetweenurl(fk_toURL_id)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlfromidx    ON linkbetweenurl(fk_fromURL_id)')
        self.con.execute("DROP INDEX   IF EXISTS rankurlididx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS rankurlididx  ON pagerank(urlid)')
        self.con.execute("REINDEX wordidx;")
        self.con.execute("REINDEX urlidx;")
        self.con.execute("REINDEX wordurlidx;")
        self.con.execute("REINDEX urltoidx;")
        self.con.execute("REINDEX urlfromidx;")
        self.con.execute("REINDEX rankurlididx;")

        # в начальный момент ранг для каждого URL равен 1
        self.con.execute('INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist')
        self.db_commit()
