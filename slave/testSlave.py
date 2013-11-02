# -*- coding: utf-8 -*-
import sys
from slave import CrawlerService
from operator import itemgetter

reload(sys)
sys.setdefaultencoding('utf-8')

frDict = {}
def callbackFunc(id, job, frList):
	if job == None:
		return 3154754605
	else:
		global frDict
		for fr in frList:
			if fr in frDict:
				frDict[fr] += 1
			else:
				frDict[fr] = 1
		sortedList = sorted(frDict.iteritems(), key=itemgetter(1), reverse=True)
		next = sortedList[0][0]
		frDict.pop(next)
		return next

accList = [
    {"user": "ligx2013_11@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_12@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_13@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_14@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_15@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_16@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_17@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_18@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_19@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_20@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_21@163.com", "pwd": "weibo123"},
    {"user": "ligx2013_22@163.com", "pwd": "weibo123"},
    ]
crawler = CrawlerService.exposed_Crawler(1, accList, callbackFunc)