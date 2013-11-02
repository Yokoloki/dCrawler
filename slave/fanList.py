# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from weiboCN import accountLimitedException

def fanListProcessing(uid, page, fetcher, dbConn, dbCur):
	url = 'http://weibo.cn/%d/follow?page=%d' % (uid, page)
	html = fetcher.fetch(url)
	soup = BeautifulSoup(html)
	if soup.find('div', attrs={"class": "tip"}) and soup.find('div', attrs={"class": "tip"}).get_text().find('首页') != -1:
		raise Exception
	if page == 1:
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
	SQL = "INSERT IGNORE INTO `following` (`followerID`, `followedID`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbFollowingList)

	SQL = "INSERT IGNORE INTO `userInfo` (`uid`, `name`, `imgUrl`) VALUES (%s, %s, %s)"
	dbCur.executemany(SQL, dbInfoList)
	
	dbConn.commit()

	return [numPage, fanDict] if page == 1 else fanDict

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