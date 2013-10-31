# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from weiboCN import accountLimitedException

def nameResolving(name, fetcher):
	url = 'http://weibo.cn/n/%s' % name
	content = fetcher.fetch(url)
	soup = BeautifulSoup(content)
	if soup.find('div', attrs={"class": "tip"}) and soup.find('div', attrs={"class": "tip"}).get_text().find('首页') != -1:
		print soup.find('div', attrs={"class": "tip"})
		print url
		raise accountLimitedException
	result = soup.find('div', attrs={'class':'tip2'})
	if result:
		url = result.find('a')['href']
		lIndex = url.find('/')
		rIndex = url.find('/follow')
		if lIndex == rIndex:
			lIndex = 0
		else:
			lIndex +=1
		return int(url[lIndex : rIndex])
	else:
		return None

def weiboATResolving(name, relatedMids, fetcher, dbConn, dbCur):
	uid = nameResolving(name, fetcher)
	if uid:
		dbWeiboATList = []
		for mid in relatedMids: dbWeiboATList.append((mid, uid))
		SQL = "INSERT IGNORE INTO `weiboAT` (`mid`, `uid`) VALUES (%s, %s)"
		dbCur.executemany(SQL, dbWeiboATList)
		dbConn.commit()
		return [uid, len(relatedMids)]
	else:
		return [-1, 0]

def commentResolving(name, lists, fetcher, dbConn, dbCur):
	uid = nameResolving(name, fetcher)
	if uid:
		dbCommentingList = []
		dbCommentReplyList = []
		dbCommentATList = []
		for cid in lists[0]: dbCommentingList.append((uid, cid))
		for cid in lists[1]: dbCommentReplyList.append((uid, cid))
		for cid in lists[2]: dbCommentATList.append((cid, uid))
		SQL = "INSERT IGNORE INTO `commenting` (`uid`, `cid`) VALUES (%s, %s)"
		dbCur.executemany(SQL, dbCommentingList)
		SQL = "UPDATE `commentInfo` SET `replyTo`=%s WHERE `cid`=%s"
		dbCur.executemany(SQL, dbCommentReplyList)
		SQL = "INSERT IGNORE INTO `commentAT` (`cid`, `uid`) VALUES (%s, %s)"
		dbCur.executemany(SQL, dbCommentATList)
		dbConn.commit()
		return [uid, len(lists[0]) + len(lists[2])]
	else:
		return [-1, 0]