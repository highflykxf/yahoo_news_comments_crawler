#-*- coding:utf-8 -*-
from google import search

def query_by_google(q, tld='com', lang='en', stop=100):
    print q
    url_list = search(q, tld=tld, lang=lang, stop=stop)
    return url_list

def query_yahoo_news(q):
    if type(q)==unicode:
        q = q.encode('utf-8')
    t_list = query_by_google('%s site:http://news.yahoo.com' %(q,), tld='com.hk', stop=500)
    result_list = [item for item in t_list if item.startswith('http://news.yahoo.com/') and 'video' not in item and 'photo' not in item]
    return result_list

def query_sina_news(q):
    if type(q)==unicode:
        q = q.encode('utf-8')
    t_list = query_by_google('%s site:http://news.sina.com.cn' %(q,), tld='com.hk', lang='cn', stop=400)
    result_list = [item for item in t_list if item.startswith('http://news.sina.com.cn/') and item.endswith('shtml')]
    return result_list
    
if __name__=='__main__':
    t_list = query_yahoo_news('mh370')
    #t_list = query_by_google('mh370')
    for item in t_list:
        print(item)