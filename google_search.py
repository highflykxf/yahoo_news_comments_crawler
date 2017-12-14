#-*- coding:utf-8 -*-
from google import search
from openpyxl import load_workbook
from crawler import *
import MySQLdb


def query_by_google(q, tld='com', lang='en', stop=100):
    print q
    url_list = search(q, tld=tld, lang=lang, stop=stop)
    return url_list


def query_yahoo_news(q):
    if type(q)==unicode:
        q = q.encode('utf-8')
    t_list = query_by_google('%s site:https://www.yahoo.com/news' %(q,), tld='com.hk', stop=100)
    result_list = [item for item in t_list if item.startswith('https://www.yahoo.com/news/') and 'video' not in item and 'photo' not in item]
    return result_list

    
if __name__=='__main__':
    conn = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='1234', db='yahoo_news', charset='utf8')
    wb = load_workbook("queries.xlsx")
    sheet = wb.get_sheet_by_name("Sheet1")
    for r_id in range(0, len(sheet['B'])):
        if r_id == 0:
            continue
        query_id = int(sheet['A'][r_id].value)
        if query_id not in [8,9,10]:
            continue
        query_str = str(sheet['B'][r_id].value)
        url_list = query_yahoo_news(query_str)
        for order_id, news_url in enumerate(url_list):
            news_dict = parse_news_title_and_content(news_url)
            if news_dict is None:
                continue
            reviews_url = urlencode(news_dict['content_id'], news_dict['comment_num'])
            parse_comments(conn, query_id, query_str, order_id+1, news_url, news_dict, reviews_url)