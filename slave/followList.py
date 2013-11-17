# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from weiboCN import accountLimitedException

def followListProcessing(uid, page, fetcher, dbConn, dbCur):
	url = 'http://weibo.cn/%d/follow?page=%d' % (uid, page)
	soup = fetcher.fetch(url)
	if page == 1:
		spanText = soup.find(attrs={"class": "tip2"}).find('span', attrs={"class": "tc"}).get_text()
		followCount = int(spanText[spanText.find('[')+1 : spanText.find(']')])
		pageInfo = soup.find(attrs={"class":"pa","id":"pagelist"})
		numPage = getPageCount(pageInfo)
	tables = soup.findAll('table')
	followDict = {}
	dbFollowingList = []
	dbInfoList = []
	for table in tables:
		imgUrl = table.find('img')['src']
		matched = table.findAll('a')
		name = matched[1].get_text()
		followID = getFollowID(matched[-1]['href'])
		if followID == -1:
			continue
		followDict[name] = followID
		dbFollowingList.append((uid, followID))
		dbInfoList.append((followID, name, imgUrl))
	SQL = "INSERT IGNORE INTO `following` (`followerID`, `followedID`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbFollowingList)

	SQL = "INSERT IGNORE INTO `userInfo` (`uid`, `name`, `imgUrl`) VALUES (%s, %s, %s)"
	dbCur.executemany(SQL, dbInfoList)
	
	dbConn.commit()

	return [followCount, numPage, followDict] if page == 1 else followDict

def getPageCount(pageInfo):
	if not pageInfo:
		return 1
	else:
		tmp = pageInfo.get_text()
		rIndex = tmp.rfind('页')
		lIndex = tmp.rfind('1/')
		return int(tmp[lIndex+2:rIndex])

def getFollowID(content):
	leftindex = content.find('uid=')
	rightindex = content.find('&rl')
	if leftindex == -1 or rightindex == -1:
		#RaiseException
		return -1
	else:
		return int(content[leftindex+4:rightindex])