#-*- coding:utf-8 -*-
import httplib2
import urllib
import json
import traceback
import bs4
from bs4 import BeautifulSoup
from goose import Goose


mainpage_url = 'http://news.yahoo.com/us/most-popular/'
comment_base_url = 'http://news.yahoo.com/_xhr/contentcomments/get_all/?'
reply_base_url = 'http://news.yahoo.com/_xhr/contentcomments/get_replies/?'
t_news_url = 'http://news.yahoo.com/ebola-victims-sister-says-hospital-denied-request-025725064.html'


def urlencode(base, param):
    param_code = urllib.urlencode(param)
    rurl = base + param_code
    return rurl

def get_response(url):
    head, content = httplib2.Http().request(url)
    return head, content

def get_news_mainpage():
    mainpage_head, mainpage_content = get_response(mainpage_url)
    return mainpage_head, mainpage_content

def parse_news_from_mainpage(mainpage_content):
    soup = BeautifulSoup(mainpage_content, from_encoding='utf-8')
    l_news = soup.find_all('div', {'class':'body-wrap'})
    r_list = []
    for n in l_news:
        if n.p:
            s_url = n.h3.a['href']
            if not s_url.startswith('http') and not s_url.startswith('/video') and not s_url.startswith('/photos') and not s_url.startswith('/blogs'):
                r_list.append(['http://news.yahoo.com' + n.h3.a['href'], n.p.string])
    return r_list

def parse_news_title_and_content(news_url):
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

def parse_comment_num(news_url):
    head, content = httplib2.Http().request(news_url)
    soup = BeautifulSoup(content.decode('utf-8'))
    c_num = soup.find('span', {'id':'total-comment-count'}).string
    if c_num:
        return int(c_num.strip())
    else:
        return 0

def set_comment_num(session, news_id, num):
    try:
        sql = 'update parallel_news set comment_num=%s where id=%s'
        cur = session.cursor()
        cur.execute(sql, (num, news_id))
        session.commit()
    except:
        traceback.print_exc()
    
def set_comment_has_reply(session, id):
    try:
        sql = 'update parallel_comment set has_reply=1 where id=%s'
        cur = session.cursor()
        cur.execute(sql, (id,))
        session.commit()
    except:
        traceback.print_exc()
        
def parse_comments(session, content_url, content_id, current_index, news_id, event_id):
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
                save_comments(session, nickname.encode('utf-8'), thumb_up_count, thumb_down_count, content.encode('utf-8'), 0, has_reply, -1, news_id, event_id, language_type)
                comment_id_db = get_comment_id(session, nickname, content.encode('utf-8'), news_id)
                if span_reply and comment_id_db!=-1:
                    reply_url = urlencode(reply_base_url, {'content_id':content_id, 'comment_id':comment_id})
                    parse_reply_comment(session, reply_url, content_id, comment_id, comment_id_db, 0, news_id, event_id, language_type)
            except:
                traceback.print_exc()
        if more_url:
            m_soup = BeautifulSoup(more_url)
            nextpage_url = urlencode(comment_base_url, {'content_id':content_id}) + '&'+ m_soup.li.span['data-query']
            current_index = current_index + len(comment_list)
            print current_index
            parse_comments(session, nextpage_url, content_id, current_index, news_id, event_id, language_type)
        else:
            return
    except:
        traceback.print_exc()

def get_comment_id(session, nickname, content, news_id):
    try:
        sql = 'select id from parallel_comment where nick=%s and content=%s and news_id=%s'
        cur = session.cursor()
        cur.execute(sql, (nickname, content.encode('utf-8'), news_id))
        r = cur.fetchone()
        if r:
            return r[0]
        else:
            return -1
    except:
        traceback.print_exc()
        return -1
    
def save_comments(session, nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, language_type, mid=''):
    cur = session.cursor()
    sql = 'insert into parallel_comment(nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, language_type, mid) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    t_list = (nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, language_type, mid)
    cur.execute(sql, t_list)
    session.commit()
        
def parse_reply_comment(session, content_url, content_id, comment_id, comment_id_db, current_index, news_id, event_id, language_type):
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
            save_comments(session, nickname.encode('utf-8'), thumb_up_count, thumb_down_count, content.encode('utf-8'), 1, 0, comment_id_db, news_id, event_id, language_type)
        except:
            traceback.print_exc()

    if more_url:
        m_soup = BeautifulSoup(more_url)
        nextpage_url = urlencode(reply_base_url, {'content_id':content_id, 'comment_id':comment_id}) + '&'+ m_soup.li.span['data-query']
        current_index = current_index + len(reply_comment_list)
        parse_reply_comment(session, nextpage_url, content_id, comment_id, comment_id_db, current_index, news_id, event_id, language_type)
    else:
        return
            
if __name__=='__main__':
    #yahoo_mainpage_head, yahoo_mainpage_content = get_news_mainpage()
    #parse_news_from_mainpage(yahoo_mainpage_content)
    #news_dict = parse_news_title_and_content(t_news_url)
    #news_c_id = news_dict['content_id']
    #param_dict = {'content_id':news_c_id, 'sortBy':'highestRated'}
    #rurl = urlencode(comment_base_url, param_dict)
    #parse_comments(rurl, news_c_id, 0)
    pass
