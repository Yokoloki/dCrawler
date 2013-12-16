# -*- coding: utf-8 -*-
import urllib2, urllib, httplib, cookielib, functools
import logging
import datetime
import lxml.html as HTML
from lxml.etree import XMLSyntaxError
from time import sleep
from bs4 import BeautifulSoup

class BoundHTTPHandler(urllib2.HTTPHandler):
    def __init__(self, source_address=None):
        urllib2.HTTPHandler.__init__(self)
        self.http_class = functools.partial(httplib.HTTPConnection, source_address=source_address)
    def http_open(self, req):
        return self.do_open(self.http_class, req)

class Fetcher(object):
    def __init__(self):
        self.logger = logging.getLogger('Fetcher')
        self.cj = cookielib.LWPCookieJar()
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)
        #self.handler = BoundHTTPHandler(source_address='127.0.0.1')
        #self.opener = urllib2.build_opener(self.cookie_processor, self.handler)
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)
        #Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19
        #Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543 Safari/419.3
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows;U;Windows NT 5.1;zh-CN;rv:1.9.2.9)Gecko/20100824 Firefox/3.6.9',
                        'Referer':'','Content-Type':'application/x-www-form-urlencoded'}

    def get_rand(self, url):
        while True:
            try:
                req = urllib2.Request(url ,urllib.urlencode({}), self.headers)
                resp = urllib2.urlopen(req)
                login_page = resp.read()
                rand = HTML.fromstring(login_page).xpath("//form/@action")[0]
                passwd = HTML.fromstring(login_page).xpath("//input[@type='password']/@name")[0]
                vk = HTML.fromstring(login_page).xpath("//input[@name='vk']/@value")[0]
                return rand, passwd, vk
            except:
                self.logger.error('get_rand error when login, try again in 0.2s')
                sleep(0.2)
     
    def login(self, username, pwd):
        self.username = username
        self.pwd = pwd
        assert self.username is not None and self.pwd is not None

        url = 'http://3g.sina.com.cn/prog/wapsite/sso/login.php?ns=1&revalid=2&backURL=http%3A%2F%2Fweibo.cn%2F&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt='
        rand, passwd, vk = self.get_rand(url)
        data = urllib.urlencode({'mobile': self.username,
                                 passwd: self.pwd,
                                 'remember': 'on',
                                 'backURL': 'http://weibo.cn/',
                                 'backTitle': '新浪微博',
                                 'vk': vk,
                                 'submit': '登录',
                                 'encoding': 'utf-8'})
        url = 'http://3g.sina.com.cn/prog/wapsite/sso/' + rand
        req = urllib2.Request(url, data, self.headers)
        response = self.opener.open(req)
        page = response.read()
        msgErr = HTML.fromstring(page).xpath("//div[contains(@class, 'msgErr')]")
        if msgErr != []:
            raise accountBannedException

        link = HTML.fromstring(page).xpath("//a/@href")[0]
        if not link.startswith('http://'): link = 'http://weibo.cn/%s' % link
        req = urllib2.Request(link, headers=self.headers)
        response = self.opener.open(req)
        self.setPage()
        self.setImg()

    def logout(self):
        url = 'http://3g.sina.com.cn/prog/wapsite/sso/loginout.php?backURL=http%3A%2F%2Fweibo.cn%2Fpub%2F%3Fvt%3D4&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt=4'
        req = urllib2.Request(url, headers = self.headers)
        response = self.opener.open(req)
        page = response.read()
        link = HTML.fromstring(page).xpath("//a/@href")[0]
        if not link.startswith('http://'): link = 'http://weibo.cn/%s' % link
        req = urllib2.Request(link, headers=self.headers)
        response = self.opener.open(req)

    def setPage(self):
        url = "http://weibo.cn/account/customize/pagesize"
        req = urllib2.Request(url, headers=self.headers)
        response = self.opener.open(req)
        page = response.read()
        settingUrl = "http://weibo.cn" + HTML.fromstring(page).xpath("//form")[0].action.strip()
        data = urllib.urlencode({'act': 'customize',
                                 'MBlogPageSize': '50',
                                 'save': '%E9%A2%84%E8%A7%88'})
        req = urllib2.Request(url, data, self.headers)
        response = self.opener.open(req)

        url = "http://weibo.cn/account/customize/save?st=2555"
        req = urllib2.Request(url, headers=self.headers)
        response = self.opener.open(req)
        page = response.read()
        #print len(HTML.fromstring(page).xpath("//div[contains(@class, 'c') and contains(@id, 'M_')]"))

    def setImg(self):
        url = "http://weibo.cn/account/customize/pic"
        req = urllib2.Request(url, headers=self.headers)
        response = self.opener.open(req)
        page = response.read()
        settingUrl = "http://weibo.cn" + HTML.fromstring(page).xpath("//form")[0].action.strip()
        data = urllib.urlencode({'act': 'customize',
                                 'ShowMblogPic': '0',
                                 'save': '%E9%A2%84%E8%A7%88'})
        req = urllib2.Request(url, data, self.headers)
        response = self.opener.open(req)

        url = "http://weibo.cn/account/customize/save?st=2555"
        req = urllib2.Request(url, headers=self.headers)
        response = self.opener.open(req)
        page = response.read()

    def fetch(self, url):
        while True:
            try:
                req = urllib2.Request(url, headers=self.headers)
                response = self.opener.open(req)
                content = response.read()
                break
            except Exception, e:
                self.logger.error('%r when fetching %s, try again in 0.2s' % (e, url))
                sleep(0.2)
        soup = BeautifulSoup(content)
        if soup.find('div', attrs={"class": "tip"}) and soup.find('div', attrs={"class": "tip"}).get_text().find('首页') != -1:
            raise accountLimitedException
        if soup.find('div', attrs={"class": "c"}) and soup.find('div', attrs={"class": "c"}).get_text().find('您的微博帐号出现异常被暂时冻结') != -1:
            raise accountFreezedException
        if soup.find('div', attrs={"class": "me"}) and soup.find('div', attrs={"class": "me"}).get_text().find('您当前访问的用户状态异常') != -1:
            raise accountErrorException
        return soup



class accountLimitedException(Exception):
    pass
class accountFreezedException(Exception):
    pass
class accountBannedException(Exception):
    pass
class accountErrorException(Exception):
    pass
class loginException(Exception):
    pass
class fetcherException(Exception):
    pass