# -*- coding: utf-8 -*-
import sys
from slave import CrawlerService

reload(sys)
sys.setdefaultencoding('utf-8')

def callbackFunc(id, job, frList):
	if job == None:
		return 3154754605
	else:
		return frList[0]
	print 'Server id: %d' % id
	print 'Job: %d' % job
	print 'FrList: %r' % frList

accList = [
    {'user': 'crawler2013_00@163.com', 'psw': '654123'},
    {'user': 'crawler2013_01@163.com', 'psw': '654123'},
    ]
crawler = CrawlerService.exposed_Crawler(1, accList, callbackFunc)