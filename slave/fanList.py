# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from weiboCN import accountLimitedException

def fanListProcessing(uid, page, fetcher):
	url = 'http://weibo.cn/%d/fans?page=%d' % (uid, page)
	soup = fetcher.fetch(url)
	if page == 1:
		spanText = soup.find(attrs={"class": "tip2"}).find('span', attrs={"class": "tc"}).get_text()
		fanCount = int(spanText[spanText.find('[')+1 : spanText.find(']')])
		pageInfo = soup.find(attrs={"class":"pa","id":"pagelist"})
		numPage = getPageCount(pageInfo)
	tables = soup.findAll('table')
	fanDict = {}
	dbFollowingList = []
	dbInfoList = []
	for table in tables:
		imgUrl = table.find('img')['src']
		matched = table.findAll('a')
		name = matched[1].get_text()
		fanID = getFanID(matched[-1]['href'])
		if fanID == -1:
			continue
		fanDict[name] = fanID
		dbFollowingList.append((fanID, uid))
		dbInfoList.append((fanID, name, imgUrl))

	return [fanCount, numPage, fanDict, dbFollowingList, dbInfoList] if page == 1 else [fanDict, dbFollowingList, dbInfoList]

def getPageCount(pageInfo):
	if not pageInfo:
		return 1
	else:
		tmp = pageInfo.get_text()
		rIndex = tmp.rfind('页')
		lIndex = tmp.rfind('1/')
		return int(tmp[lIndex+2:rIndex])

def getFanID(content):
	leftindex = content.find('uid=')
	rightindex = content.find('&rl')
	if leftindex == -1 or rightindex == -1:
		return -1
	else:
		return int(content[leftindex+4:rightindex])