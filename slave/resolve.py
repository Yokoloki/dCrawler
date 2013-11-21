# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from weiboCN import accountLimitedException

def nameResolving(name, fetcher, dbConn, dbCur):
	SQL = "SELECT `uid` from `userInfo` WHERE `name`=\'%s\'" % name
	len = dbCur.execute(SQL)
	if len > 0:
		uid = dbCur.fetchone()
		return [uid[0], []]
	url = 'http://weibo.cn/n/%s' % name
	soup = fetcher.fetch(url)
	result = soup.find('div', attrs={'class':'tip2'})
	if result:
		url = result.find('a')['href']
		lIndex = url.find('/')
		rIndex = url.find('/follow')
		if lIndex == rIndex:
			lIndex = 0
		else:
			lIndex +=1
		uid = int(url[lIndex : rIndex])
		imgUrl = result.previous_sibling.find('img')['src']
		dbUserInfo = [(uid, name, imgUrl)]
		return [uid, dbUserInfo]
	else:
		return [None, []]

def weiboATResolving(name, relatedMids, fetcher, dbConn, dbCur):
	[uid, dbUserInfo] = nameResolving(name, fetcher, dbConn, dbCur)
	if uid:
		dbWeiboAtList = [(mid, uid) for mid in relatedMids]
		#SQL = "INSERT IGNORE INTO `weiboAT` (`mid`, `uid`) VALUES (%s, %s)"
		#dbCur.executemany(SQL, dbWeiboAtList)
		#dbConn.commit()
		return [uid, len(relatedMids), dbWeiboAtList, dbUserInfo]
	else:
		return [-1, 0, [], []]

def commentResolving(name, lists, fetcher, dbConn, dbCur):
	[uid, dbUserInfo] = nameResolving(name, fetcher, dbConn, dbCur)
	if uid:
		dbCommentingList = [(uid, cid) for cid in lists[0]]
		dbCommentReplyList = [(uid, cid) for cid in lists[1]]
		dbCommentATList = [(cid, uid) for cid in lists[2]]
		#SQL = "INSERT IGNORE INTO `commenting` (`uid`, `cid`) VALUES (%s, %s)"
		#dbCur.executemany(SQL, dbCommentingList)
		#SQL = "UPDATE `commentInfo` SET `replyTo`=%s WHERE `cid`=%s"
		#dbCur.executemany(SQL, dbCommentReplyList)
		#SQL = "INSERT IGNORE INTO `commentAT` (`cid`, `uid`) VALUES (%s, %s)"
		#dbCur.executemany(SQL, dbCommentATList)
		#dbConn.commit()
		return [uid, len(lists[0]) + len(lists[2]), dbCommentingList, dbCommentReplyList, dbCommentATList, dbUserInfo]
	else:
		return [-1, 0, [], [], [], []]