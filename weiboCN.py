# -*- coding: utf-8 -*-
import urllib2
import urllib
import cookielib
import sys
import datetime
import lxml.html as HTML
from lxml.etree import XMLSyntaxError
from time import sleep
reload(sys)
sys.setdefaultencoding("utf-8")
class Fetcher(object):
    def __init__(self):
        self.cj = cookielib.LWPCookieJar()
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)
        

        #Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19
        self.headers = {'User-Agent':'Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543 Safari/419.3',
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
                'so try again in 0.2s'
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
        link = HTML.fromstring(page).xpath("//a/@href")[0]
        if not link.startswith('http://'): link = 'http://weibo.cn/%s' % link
        req = urllib2.Request(link, headers=self.headers)
        response = self.opener.open(req)

        #print '%s login success!'%self.username
    def logout(self):
        url = 'http://3g.sina.com.cn/prog/wapsite/sso/loginout.php?backURL=http%3A%2F%2Fweibo.cn%2Fpub%2F%3Fvt%3D4&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt=4'
        req = urllib2.Request(url, headers = self.headers)
        response = self.opener.open(req)
        page = response.read()
        link = HTML.fromstring(page).xpath("//a/@href")[0]
        if not link.startswith('http://'): link = 'http://weibo.cn/%s' % link
        req = urllib2.Request(link, headers=self.headers)
        response = self.opener.open(req)
         
    def fetch(self, url):
        while True:
            try:
                req = urllib2.Request(url, headers=self.headers)
                response = self.opener.open(req)
                return response.read()
            except:
                print 'try again in 0.2s'
                sleep(0.2)

class accountLimitedException(Exception):
    pass