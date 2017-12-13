# -*- coding:utf-8 -*-
import httplib2
import json
import traceback
import re
from bs4 import BeautifulSoup
# from goose import Goose
from selenium import webdriver
import MySQLdb
import time
import datetime

mainpage_url = 'http://news.yahoo.com/us/most-popular/'


def urlencode(context_id, comment_num):
    rurl = 'https://www.yahoo.com/news/_td/api/resource/canvass.getMessageListForContext_ns' \
           ';context=' + context_id + ';count=' + str(comment_num) + \
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
    print news_url
    # 设置chrome选项：不加载图片
    chromeOptions = webdriver.ChromeOptions()
    prefs = {"profile.managed_default_content_settings.images": 2}
    chromeOptions.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    driver.set_script_timeout(30)
    driver.get(news_url)
    time.sleep(20)
    webpage_content = driver.page_source
    soup = BeautifulSoup(webpage_content, "html.parser")

    title = soup.find('h1', {'itemprop': 'headline'}).string
    news_time = soup.find('time', {'itemprop': 'datePublished'}).string
    l_p = soup.find_all('p', {'type': "text"})
    c_num = 2
    # if soup.find(text=re.compile(r'\d+\sreactions')) != None:
    #     c_num = re.findall("\d+", str(soup.find(text=re.compile(r'\d+\sreactions')).encode('utf-8')))[0]
    press_name = soup.find('span', {'class': 'provider-link'}).string
    content_id = soup.find('article', {'itemprop': 'articleBody'})
    if content_id is None:
        return None
    c_id = content_id['data-uuid']

    l_content = []
    for p in l_p:
        l_content.append(p['content'])
    content = ''
    content = '\n'.join(l_content).encode('utf-8')

    news_dict = {}
    news_dict['title'] = title.encode('utf-8')
    news_dict['news_url'] = news_url
    news_dict['content'] = content
    news_dict['comment_num'] = int(c_num)
    news_dict['press_name'] = press_name.encode('utf-8')
    news_dict['content_id'] = c_id
    news_dict['time'] = news_time
    driver.close()
    return news_dict


# parse_comments(conn, query_id, query_str, order_id+1, news_url, news_dict, reviews_url)
def parse_comments(session, query_id, query_str, order_id, news_url, news_dict, reviews_url):
    try:
        head, content = httplib2.Http().request(reviews_url)
        json_data = json.loads(content)
        data = json_data['data']

        rurl_new = urlencode(news_dict['content_id'], data['total']['count'])
        head, content = httplib2.Http().request(rurl_new)
        json_data = json.loads(content)
        data = json_data['data']
        # readingUserCount
        readingUserCount = 0
        if data['userActivityNotification'] is not None or data['userActivityNotification']['readingUsersCount'] is not None:
            readingUserCount = data['userActivityNotification']['readingUsersCount']

        # sentiment count
        sentiment_pos = 0
        sentiment_neu = 0
        sentiment_neg = 0
        if data.has_key('sentiments'):
            sentiments = data['sentiments']
            for s_c in sentiments:
                if s_c['sentiment'] == "POSITIVE":
                    sentiment_pos = int(s_c['count'])
                elif s_c['sentiment'] == "NEUTRAL":
                    sentiment_neu = int(s_c['count'])
                elif s_c['sentiment'] == "NEGATIVE":
                    sentiment_neg = int(s_c['count'])

        canvassMessages = [''] * len(data['canvassMessages'])
        for c_id, comment in enumerate(data['canvassMessages']):
            canvassMessage = {}
            canvassMessage['sentimentLabel'] = ""
            if comment['meta'].has_key('sentimentLabel'):
                canvassMessage['sentimentLabel'] = comment['meta']['sentimentLabel']
            canvassMessage['details'] = comment['details']['userText']
            canvassMessage['reactionStats'] = comment['reactionStats']
            canvassMessages[c_id] = canvassMessage
        save_comments(session, query_id, query_str, order_id, news_url, news_dict['title'], news_dict['content'],
                      data['total']['count'],
                      news_dict['press_name'], news_dict['content_id'], readingUserCount, sentiment_pos, sentiment_neu,
                      sentiment_neg, canvassMessages)
    except:
        traceback.print_exc()


def save_comments(session, query_id, query_str, order_id, news_url, title, content, comment_num, press_name, content_id,
                  reading_user_count,
                  sentiment_pos, sentiment_neu, sentiment_neg, canvass_messages):
    # cur = session.cursor()
    sql = 'insert into news_comment(query_id, query_str, order_id, news_url, title, content, comment_num, ' \
          'press_name, content_id, reading_user_count, sentiment_pos, sentiment_neu, sentiment_neg, ' \
          'canvass_messages) values'
    t_list = (
        query_id, query_str, order_id, news_url, title, content, str(comment_num), press_name, str(content_id),
        str(reading_user_count), str(sentiment_pos), str(sentiment_neu), str(sentiment_neg),
        str(canvass_messages))
    print sql
    print t_list
    # cur.execute(sql, t_list)
    # session.commit()


if __name__ == '__main__':
    conn = MySQLdb.connect(host='localhost', port=3306, user='root', passwd='1234', db='yahoo_news', charset='utf8')
    yahoo_mainpage_head, yahoo_mainpage_content = get_news_mainpage()
    url_list = parse_news_from_mainpage(yahoo_mainpage_content)
    acc_num = 5
    for acc_id, news_url in enumerate(url_list):
        if acc_id < acc_num:
            continue
        news_dict = parse_news_title_and_content(news_url)
        if news_dict is None:
            continue
        rurl = urlencode(news_dict['content_id'], news_dict['comment_num'])
        parse_comments(conn, rurl, news_dict)
