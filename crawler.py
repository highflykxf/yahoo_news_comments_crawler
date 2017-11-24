# -*- coding:utf-8 -*-
import httplib2
import urllib
import json
import traceback
import re
import bs4
from bs4 import BeautifulSoup
# from goose import Goose
from selenium import webdriver
import MySQLdb


mainpage_url = 'http://news.yahoo.com/us/most-popular/'


def urlencode(context_id, comment_num):
    rurl = 'https://www.yahoo.com/news/_td/api/resource/canvass.getMessageListForContext_ns' \
           ';context=' + context_id + ';count=' + comment_num + \
           ';index=null' \
           ';lang=en-US' \
           ';namespace=yahoo_content' \
           ';oauthConsumerKey=frontpage.oauth.canvassKey' \
           ';oauthConsumerSecret=frontpage.oauth.canvassSecret' \
           ';rankingProfile=canvassHalfLifeDecayProfile' \
           ';region=US' \
           ';sentiment=true' \
           ';sortBy=popular' \
           ';type=null' \
           ';userActivity=true' \
           '?bkt=newsdmcntr' \
           '&device=desktop' \
           '&feature=cacheContentCanvas' \
           '%2CvideoDocking' \
           '%2CnewContentAttribution' \
           '%2Clivecoverage' \
           '%2Cfeaturebar' \
           '%2CdeferModalCluster' \
           '%2Cc2sGa' \
           '%2CcanvassOffnet' \
           '%2CnewLayout' \
           '%2CntkFilmstrip' \
           '%2Csidepic%2CautoNotif' \
           '%2CfauxdalNewLayout%2CbtmVidPlaylist%2CcacheContentCanvasAds' \
           '%2CfauxdalStream' \
           '&intl=us&lang=en-US&partner=none&prid=7qs0n7pd1euej&region=US&site=fp&tz=Asia' \
           '%2FShanghai&ver=2.0.11087' \
           '&returnMeta=true'
    return rurl


def get_response(url):
    head, content = httplib2.Http().request(url)
    return head, content


def get_news_mainpage():
    mainpage_head, mainpage_content = get_response(mainpage_url)
    return mainpage_head, mainpage_content


def parse_news_from_mainpage(mainpage_content):
    soup = BeautifulSoup(mainpage_content, from_encoding='utf-8')
    # l_news = soup.find_all('div', {'class':'body-wrap'})
    # l_news = soup.find_all(has_class_and_contain_keys)
    l_news = soup.find_all('div', {'class': re.compile(r'^Ov')})
    r_list = []
    for n in l_news:
        if n.p:
            s_url = n.h3.a['href']
            if not s_url.startswith('http') and not s_url.startswith('/video') and not s_url.startswith(
                    '/photos') and not s_url.startswith('/blogs'):
                # r_list.append(['http://news.yahoo.com' + n.h3.a['href'], n.p.string])
                r_list.append('http://news.yahoo.com' + n.h3.a['href'])
    return r_list


def parse_news_title_and_content(news_url):
    # 设置chrome选项：不加载图片
    chromeOptions = webdriver.ChromeOptions()
    prefs = {"profile.managed_default_content_settings.images": 2}
    chromeOptions.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    driver.set_script_timeout(30)
    driver.get(news_url)
    webpage_content = driver.page_source
    soup = BeautifulSoup(webpage_content, "html.parser")

    title = soup.find('h1', {'itemprop': 'headline'}).string
    news_time = soup.find('time', {'itemprop': 'datePublished'}).string
    l_p = soup.find_all('p', {'type': "text"})
    c_num = re.findall("\d+", str(soup.find(text=re.compile(r'\d+\sreactions')).encode('utf-8')))[0]
    press_name = soup.find('span', {'class': 'provider-link'}).string
    content_id = soup.find('article', {'itemprop': 'articleBody'})
    c_id = content_id['data-uuid']

    l_content = []
    for p in l_p:
        if p.content:
            l_content.append(p['content'])
    content = ''
    content = '\n'.join(l_content).encode('utf-8')

    news_dict = {}
    news_dict['title'] = title.encode('utf-8')
    news_dict['content'] = content
    news_dict['comment_num'] = int(c_num)
    news_dict['press_name'] = press_name.encode('utf-8')
    news_dict['content_id'] = c_id
    news_dict['time'] = news_time
    driver.close()
    return news_dict


def parse_comments(content_url):
    print content_url
    sentiment_count = {}
    canvassMessages = []
    try:
        head, content = httplib2.Http().request(content_url)
        json_data = json.loads(content)
        data = json_data['data']
        total_count = data['total']['count']
        sentiments = data['sentiments']
        for s_c in sentiments:
            sentiment_count[s_c['sentiment']] = s_c['count']

        for c_id, comment in enumerate(data['canvasMessages']):
            canvassMessage = {}
            canvassMessage['sentimentLabel'] = comment['sentimentLabel']
            canvassMessage['details'] = comment['details']['userText']
            canvassMessage['reactionStats'] = comment['reactionStats']
            canvassMessages[c_id] = canvassMessage
            # try:
            #     save_comments(session, nickname.encode('utf-8'), thumb_up_count, thumb_down_count,
            #                   content.encode('utf-8'), 0, has_reply, -1, news_id, event_id, language_type)
            #     comment_id_db = get_comment_id(session, nickname, content.encode('utf-8'), news_id)
            #     if span_reply and comment_id_db != -1:
            #         reply_url = urlencode(reply_base_url, {'content_id': content_id, 'comment_id': comment_id})
            #         parse_reply_comment(session, reply_url, content_id, comment_id, comment_id_db, 0, news_id, event_id,
            #                             language_type)
            # except:
            #     traceback.print_exc()
    except:
        traceback.print_exc()


def save_comments(session, nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id,
                  event_id, language_type, mid=''):
    cur = session.cursor()
    sql = 'insert into parallel_comment(nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, language_type, mid) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    t_list = (
        nick, thumb_up, thumb_down, content, is_reply, has_reply, reply_comment_id, news_id, event_id, language_type,
        mid)
    cur.execute(sql, t_list)
    session.commit()


if __name__ == '__main__':
    conn = MySQLdb.connect(host='seis10.se.cuhk.edu.hk', port=3306, user='bshi', passwd='20141031shib', db='bshi', charset='utf8')
    yahoo_mainpage_head, yahoo_mainpage_content = get_news_mainpage()
    url_list = parse_news_from_mainpage(yahoo_mainpage_content)
    for news_url in url_list:
        news_dict = parse_news_title_and_content(news_url)
        news_c_id = news_dict['content_id']
        rurl = urlencode(news_c_id, news_dict['comment_num'])
        parse_comments(rurl)
