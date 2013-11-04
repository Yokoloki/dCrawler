# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from weiboCN import accountLimitedException

def commentProcessing(mid, page, frDict, nameDict, fetcher, dbConn, dbCur):
	url = 'http://weibo.cn/comment/%s?page=%d' % (mid, page)
	content = fetcher.fetch(url)
	soup = BeautifulSoup(content)
	if soup.find('div', attrs={"class": "tip"}) and soup.find('div', attrs={"class": "tip"}).get_text().find('首页') != -1:
		raise accountLimitedException
	if page == 1:
		pInfo = soup.find(attrs={"class": "pa", "id": "pagelist"})
		pageCount = getPageCount(pInfo)

	rawInfo = soup.findAll(attrs={"class":"c","id":True})
	dbCommentList = []
	dbCommentATList = []
	dbCommentingList = []
	dbCommentOfList = []
	#name -> [[posted comment], [replied by], [comment ating]]
	unresolvedDict = {}
	staticsDict = {}
	#Resolve reply and @
	#Ignore the first one which is the weibo itself
	for info in rawInfo[1:]:
		cid = int(info['id'][2:])
		dbCommentOfList.append((cid, mid))
		commentatorInfo = info.find('a')
		commentatorName = commentatorInfo.get_text()
		commentatorID = frDict.get(commentatorName) or nameDict.get(commentatorName)
		if not commentatorID:
			href = commentatorInfo['href']
			if href.find('/u/') != -1:
				commentatorID = int(href[href.find('/u/')+3 : href.find('?')])
			else:
				if commentatorName in unresolvedDict:
					unresolvedDict[commentatorName][0].append(cid)
				else:
					unresolvedDict[commentatorName] = [[cid], [], []]
		if commentatorID:
			dbCommentingList.append((commentatorID, cid))
			if commentatorID in staticsDict:
				staticsDict[commentatorID] += 1
			else:
				staticsDict[commentatorID] = 1
		commentInfo = info.find('span', attrs={'class':'ctt'})
		isReply = False
		if commentInfo.get_text().find('回复@') == 0 and commentInfo.find('a'):
			isReply = True
			replytoInfo = commentInfo.find('a')
			replytoName = replytoInfo.string[1:]
			replytoUID = frDict.get(replytoName) or nameDict.get(replytoName)
			if not replytoUID:
				replytoURL = replytoInfo['href']
				if replytoURL.find('/u/') != -1:
					replytoUID = int(replytoURL[replytoURL.find('/u/')+3 : replytoURL.find('?')])
				else:
					if replytoName in unresolvedDict:
						unresolvedDict[replytoName][1].append(cid)
					else:
						unresolvedDict[replytoName] = [[], [cid], []]

		commentContent = commentInfo.get_text()
		atInfo = commentInfo.findAll('a')
		atList = []
		if atInfo != []:
			if isReply and len(atInfo)>1:
				atList = getATList(atInfo[1:], frDict, nameDict)
			elif not isReply:
				atList = getATList(atInfo, frDict, nameDict)
			for at in atList:
				if at[1]:
					dbCommentATList.append((cid, at[1]))
					if at[1] in staticsDict:
						staticsDict[at[1]] += 1
					else:
						staticsDict[at[1]] = 1
				else:
					if at[0] in unresolvedDict:
						unresolvedDict[at[0]][2].append(cid)
					else:
						unresolvedDict[at[0]] = [[], [], [cid]]
		timeInfo = info.find('span', attrs={'class':'ct'}).get_text()
		timeString = timeInfo[:timeInfo.find('来自')].strip()
		commentTime = timeFormatting(timeString)
		if isReply:
			if replytoUID:
				dbCommentList.append((cid, commentTime, commentContent, 1, replytoUID))
			else:
				dbCommentList.append((cid, commentTime, commentContent, 1, 0))
		else:
			dbCommentList.append((cid, commentTime, commentContent, 0, 0))
	SQL = "INSERT IGNORE INTO `commentOf` (`cid`, `mid`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbCommentOfList)
	SQL = "INSERT IGNORE INTO `commenting` (`uid`, `cid`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbCommentingList)
	SQL = "INSERT IGNORE INTO `commentInfo` (`cid`, `time`, `content`, `isReply`, `replyTo`) VALUES (%s, %s, %s, %s, %s)"
	dbCur.executemany(SQL, dbCommentList)
	SQL = "INSERT IGNORE INTO `commentAT` (`cid`, `uid`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbCommentATList)
	dbConn.commit()
	return [unresolvedDict, staticsDict, pageCount] if page == 1 else [unresolvedDict, staticsDict]

def getPageCount(pInfo):
	if not pInfo:
		return 1
	else:
		tmp = pInfo.get_text()
		rIndex = tmp.rfind('页')
		lIndex = tmp.rfind('1/')
		return int(tmp[lIndex+2:rIndex])

def getATList(atInfo, frDict, nameDict):
	atInfoList = []
	for info in atInfo:
		if info.get_text()[0] != '@' or info.get_text().find(' ') != -1:
			continue
		atName = info.get_text()[1:]
		atUID = frDict.get(atName) or nameDict.get(atName)
		if not atUID:
			atURL = info['href']
			if atURL.find('/u/') != -1:
				atUID = atURL[atURL.find('/u/')+3 : atURL.find('?')]
		atInfoList.append((atName, atUID))
	return atInfoList


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