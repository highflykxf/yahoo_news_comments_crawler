#-*- coding:utf-8 -*-
from google_search import *
import MySQLdb
import datetime
import traceback
import httplib2
import urllib
import json
import bs4
from bs4 import BeautifulSoup
from goose import Goose


mainpage_url = 'http://news.yahoo.com/us/most-popular/'
comment_base_url = 'http://news.yahoo.com/_xhr/contentcomments/get_all/?'
reply_base_url = 'http://news.yahoo.com/_xhr/contentcomments/get_replies/?'
t_news_url = 'http://news.yahoo.com/ebola-victims-sister-says-hospital-denied-request-025725064.html'
news_table_name = 'news_mh370'
comment_table_name = 'comment_mh370'
event_table_name = 'event_mh370'


def urlencode(base, param):
    param_code = urllib.urlencode(param)
    rurl = base + param_code
    return rurl

def get_response(url):
    head, content = httplib2.Http().request(url)
    return head, content

    
class News_extract(object):
    def __init__(self, session, event_id, english_keywords):
        self.session = session
        self.event_id = event_id
        self.english_keywords = english_keywords

    def extract_eng_news(self):
        for key_word in self.english_keywords:
            url_list = query_yahoo_news(key_word)
            for news_url in url_list:
                print news_url
                try:
                    self.crawl_yahoo_news_webpage(news_url)
                except:
                    traceback.print_exc()
                    continue

    def crawl_yahoo_news_webpage(self, news_url):
        news_dict = self.parse_news_title_and_content(news_url)
        flag = self.is_news_exist(news_url)
        if not flag:
            self.save_news(news_url, news_dict['title'], news_dict['content'], news_dict['time'], 0, news_dict['press_name'], news_dict['comment_num'], datetime.datetime.now(), news_dict['content_id'], self.event_id)

        news_c_id = news_dict['content_id']
        news_time = datetime.datetime.strptime(news_dict['time'], '%B %d, %Y %I:%M %p')
        param_dict = {'content_id':news_c_id, 'sortBy':'highestRated'}
        rurl = urlencode(comment_base_url, param_dict)
        news_id = self.get_news_id(news_url)
        flag = self.get_news_crawl_flag(news_url)
        if flag==0:
            self.parse_comments(self.session, rurl, news_c_id, 0, news_id, self.event_id, news_time)
            self.set_news_crawl_flag(news_url)

    def parse_news_title_and_content(self, news_url):
        head, content = httplib2.Http().request(news_url)
        soup = BeautifulSoup(content.decode('utf-8'))
        title = soup.find('h1', {'class':'headline'}).string
        news_time = soup.find('abbr').string
        l_p = soup.find_all('p', {'class':False})
        c_num = soup.find('span', {'id':'total-comment-count'}).string
        press_name = soup.find('img', {'class':'provider-img'})
        content_id = soup.find('section', {'id':'mediacontentstory'})
        c_id = content_id['data-uuid']
        p_name = ''
        if press_name:
            p_name = press_name['alt']
        else:
            p_span = soup.find('span', {'class':'provider-name'})
            if p_span:
                p_name = p_span.string
        l_content = []
        for p in l_p:
            if len(p.contents) > 0:
                if p.string:
                    l_content.append(p.string)
        content = ''
        content = '\n'.join(l_content).encode('utf-8')
        news_dict = {}
        news_dict['title'] = title.encode('utf-8')
        news_dict['content'] = content
        news_dict['comment_num'] = int(c_num)
        news_dict['press_name'] = p_name.encode('utf-8')
        news_dict['content_id'] = c_id
        news_dict['time'] = news_time
        return news_dict

    def parse_comments(self, session, content_url, content_id, current_index, news_id, event_id, news_time):
        print content_url
        try:
            head, content = httplib2.Http().request(content_url)
            j_data = json.loads(content)
            more_url = j_data['more']
            soup = BeautifulSoup(j_data['commentList'])
            comment_list = soup.find_all('li', {'data-uid':True})

            for comment in comment_list:
                if not comment.has_key('data-cmt'):
                    comment_id=''
                else:
                    comment_id = comment['data-cmt']
                span_nickname = comment.find('span', {'class':'int profile-link'})
                span_timestamp = comment.find('span', {'class':'comment-timestamp'})
                p_comment_content = comment.find('p', {'class': 'comment-content'})
                div_thumb_up = comment.find('div', {'id':'up-vote-box'})
                div_thumb_down = comment.find('div', {'id':'down-vote-box'})
                nickname = span_nickname.string
                timestamp = ''
                if span_timestamp:
                    timestamp = span_timestamp.string
                content = '\n'.join([x.string.strip() for x in p_comment_content.contents if x.string])
                thumb_up_count = int(div_thumb_up.span.string)
                thumb_down_count = int(div_thumb_down.span.string)

                span_reply = comment.find('span', {'class':'replies int'})
                has_reply = 0
                if span_reply:
                    has_reply = 1
                try:
                    self.save_comments(session, nickname.encode('utf-8'), thumb_up_count, thumb_down_count, content.encode('utf-8'), 0, has_reply, -1, news_id, event_id, news_time)
                    
                    if span_reply:
                        comment_id_db = self.get_comment_id(session, nickname, content.encode('utf-8'), news_id)
                        if comment_id_db != -1:
                            reply_url = urlencode(reply_base_url, {'content_id':content_id, 'comment_id':comment_id})
                            self.parse_reply_comment(session, reply_url, content_id, comment_id, comment_id_db, 0, news_id, event_id, news_time)
                except:
                    traceback.print_exc()
            if more_url:
                m_soup = BeautifulSoup(more_url)
                nextpage_url = urlencode(comment_base_url, {'content_id':content_id}) + '&'+ m_soup.li.span['data-query']
                current_index = current_index + len(comment_list)
                print current_index
                self.parse_comments(session, nextpage_url, content_id, current_index, news_id, event_id, news_time)
            else:
                return
        except:
            traceback.print_exc()

    def parse_reply_comment(self, session, content_url, content_id, comment_id, comment_id_db, current_index, news_id, event_id, news_time):
        print content_url
        head, content = httplib2.Http().request(content_url)
        j_data = json.loads(content)
        more_url = j_data['more']
        soup = BeautifulSoup(j_data['commentList'])
        reply_comment_list = soup.find_all('li', {'data-uid':True})

        for comment in reply_comment_list:
            span_nickname = comment.find('span', {'class':'int profile-link'})
            span_timestamp = comment.find('span', {'class':'comment-timestamp'})
            p_comment_content = comment.find('p', {'class': 'comment-content'})
            div_thumb_up = comment.find('div', {'id':'up-vote-box'})
            div_thumb_down = comment.find('div', {'id':'down-vote-box'})
            nickname = span_nickname.string
            timestamp = span_timestamp.string
            content = '\n'.join([x.string.strip() for x in p_comment_content.contents if x.string])
            thumb_up_count = int(div_thumb_up.span.string)
            thumb_down_count = int(div_thumb_down.span.string)
            try:
                self.save_comments(session, nickname.encode('utf-8'), thumb_up_count, thumb_down_count, content.encode('utf-8'), 1, 0, comment_id_db, news_id, event_id, news_time)
            except:
                traceback.print_exc()

        if more_url:
            m_soup = BeautifulSoup(more_url)
            nextpage_url = urlencode(reply_base_url, {'content_id':content_id, 'comment_id':comment_id}) + '&'+ m_soup.li.span['data-query']
            current_index = current_index + len(reply_comment_list)
            self.parse_reply_comment(session, nextpage_url, content_id, comment_id, comment_id_db, current_index, news_id, event_id, news_time)
        else:
            return
            
    def get_news_id(self, news_url):
        sql = 'select id from ' + news_table_name + ' where url=%s'
        cur = self.session.cursor()
        cur.execute(sql, (news_url,))
        return cur.fetchone()[0]

    def get_comment_id(self, session, nickname, content, news_id):
        try:
            sql = 'select id from ' + comment_table_name + ' where nick=%s and content=%s and news_id=%s'
            cur = session.cursor()
            cur.execute(sql, (nickname, content, news_id))
            r = cur.fetchone()
            if r:
                return r[0]
            else:
                return -1
        except:
            traceback.print_exc()
            return -1
        
    def save_news(self, url, title, content, time_text, comment_crawl_flag, press, comment_num, crawl_timestamp, content_id, event_id):
        sql = 'insert into '+ news_table_name + '(url, title, content, time_text, comment_crawl_flag, press, comment_num, crawl_timestamp, content_id, event_id) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        t_list = (url, title, content, time_text, comment_crawl_flag, press, comment_num, crawl_timestamp, content_id, event_id)
        cur = self.session.cursor()
        cur.execute(sql, t_list)
        self.session.commit()

    def save_comments(self, session, nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, news_time):
        cur = session.cursor()
        sql = 'insert into '+ comment_table_name + '(nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, stime) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        t_list = (nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, news_time)
        cur.execute(sql, t_list)
        session.commit()
        
    def is_news_exist(self, url):
        sql = 'select * from ' + news_table_name + ' where url=%s'
        cur = self.session.cursor()
        cur.execute(sql, (url,))
        r = cur.fetchone()
        return r

    def set_news_crawl_flag(self, url):
        sql = 'update ' + news_table_name + ' set comment_crawl_flag=1 where url=%s'
        cur = self.session.cursor()
        cur.execute(sql, (url,))
        self.session.commit()

    def get_news_crawl_flag(self, url):
        sql = 'select comment_crawl_flag from  ' + news_table_name + '  where url=%s'
        cur = self.session.cursor()
        cur.execute(sql, (url,))
        r = cur.fetchone()
        if r:
            return r[0]
        else:
            return 1

class Extractor(object):
    def __init__(self, session):
        self.session = session

    def get_event_list(self,):
        sql = 'select * from '+ event_table_name + '  where is_crawled_flag=0'
        cur = self.session.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql,)
        r_l = cur.fetchall()
        return r_l

    def get_event(self, event_id):
        sql = 'select * from  '+ event_table_name + '  where id=%s'
        cur = self.session.cursor(MySQLdb.cursors.DictCursor)
        cur.execute(sql,(event_id,))
        r_l = cur.fetchone()
        return r_l

    def set_news_crawl_flag(self, event_id):
        sql = 'update '+ event_table_name + ' set is_crawled_flag=1 where id=%s'
        cur = self.session.cursor()
        cur.execute(sql,(event_id,))
        self.session.commit()

    def extract(self,):
        event_list = self.get_event_list()
        for event in event_list:
            event_id = event['id']
            event_name = event['event_name']
            english_keywords = [x.strip() for x in (event['en_keyword'].split('\\'))]
            obj = News_extract(self.session, event_id, english_keywords)
            print '%s:%s' % (event_name, english_keywords)            
            obj.extract_eng_news()
            self.set_news_crawl_flag(event_id)

    def extract(self, event_id):
        event = self.get_event(event_id)
        event_id = event['id']
        event_name = event['event_name']
        english_keywords = [x.strip() for x in (event['en_keyword'].split('\\'))]
        obj = News_extract(self.session, event_id, english_keywords)
        print '%s:%s' % (event_name, english_keywords)            
        obj.extract_eng_news()
        self.set_news_crawl_flag(event_id)
    
if __name__=='__main__':
    #conn = MySQLdb.connect(host='127.0.0.1', port=9870, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    conn = MySQLdb.connect(host='seis10.se.cuhk.edu.hk', port=3306, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    e = Extractor(conn)
    e.extract(1)
    conn.close()
    print 'Extraction Completed!'
    
    #english_keywords=['mh370',]
    #obj = News_extract(conn, event_id=1, english_keywords=english_keywords)
    #obj.extract_eng_news()
    #conn.close()
