# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from weiboCN import accountLimitedException

def praiseProcessing(mid, page, fetcher, dbConn, dbCur):
	url = 'http://weibo.cn/attitude/%s?page=%d' % (mid, page)
	content = fetcher.fetch(url)
	soup = BeautifulSoup(content)
	if soup.find('div', attrs={"class": "tip"}) and soup.find('div', attrs={"class": "tip"}).get_text().find('首页') != -1:
		raise accountLimitedException
	if page == 1:
		pInfo = soup.find(attrs={"class": "pa", "id": "pagelist"})
		pageCount = getPageCount(pInfo)

	rawInfo = soup.findAll(attrs={"class":"c"})
	dbPraisedByList = []
	#name -> uid
	nameDict = {}
	for info in rawInfo:
		if not info.find('span', recursive=False):
			continue
		link = info.find('a')
		name = link.get_text()
		if info.find('img'):
			name = name[1:-1]
		linkUrl = link['href']
		uid = int(linkUrl[linkUrl.find('/u/')+3 : linkUrl.find('?')])
		time = timeFormatting(info.find('span').get_text())
		nameDict[name] = uid
		dbPraisedByList.append((mid, uid, time))

	SQL = "INSERT IGNORE INTO `praisedBy` (`mid`, `uid`, `time`) VALUES (%s, %s, %s)"
	dbCur.executemany(SQL, dbPraisedByList)
	dbConn.commit()

	return [pageCount, nameDict] if page == 1 else nameDict

def getPageCount(pInfo):
	if not pInfo:
		return 1
	else:
		tmp = pInfo.get_text()
		rIndex = tmp.rfind('页')
		lIndex = tmp.rfind('1/')
		return int(tmp[lIndex+2:rIndex])

def timeFormatting(timeString):
	currTime = datetime.now()
	if timeString.find('分钟前') != -1:
		deltaMin = int(timeString[:timeString.find('分钟前')])
		postTime = currTime - timedelta(minutes=deltaMin)
		return postTime.strftime('%Y-%m-%d %H:%M:00')
	elif timeString.find('今天') != -1:
		return currTime.strftime('%Y-%m-%d') + ' %s:00' % timeString[-5:]
	elif timeString.find('月') != -1:
		month = timeString[: timeString.find('月')]
		day = timeString[timeString.find('月')+1 : timeString.find('日')]
		return currTime.strftime('%Y-') + '%s-%s %s:00' % (month, day, timeString[-5:])
	else:
		return timeString