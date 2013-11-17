# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from weiboCN import accountLimitedException

def contentProcessing(uid, page, frDict, fetcher, dbConn, dbCur):
	url = 'http://weibo.cn/%d?page=%d' % (uid, page)
	soup = fetcher.fetch(url)
	if page == 1:
		#CrawlPersonalInfo()
		pInfo = soup.find(attrs={"class": "pa", "id": "pagelist"})
		pageCount = getPageCount(pInfo)

	rawInfo = soup.findAll(attrs={"class":"c","id":True})
	midListWithPraise = []
	midListWithRepost = []
	midListWithComment = []

	dbPostingList = []
	dbWeiboInfoList = []
	dbWeiboAtList = []
	unresolvedATDict = {}
	staticsDict = {}
	for info in rawInfo:
		mid = info['id'][2:]
		dbPostingList.append((uid, mid))
		divs = info.findAll('div')
		isRepost = len(divs) > 2
		if isRepost:
			originDiv = divs[-2]
			originCommentURL = originDiv.findAll('a')[-1]['href']
			lIndex = originCommentURL.find('/comment/')
			rIndex = originCommentURL.find('?')
			originMid = originCommentURL[lIndex+9 :rIndex]
			contentDiv = divs[-1]
			contentDiv.find('span').extract()
		else:
			contentDiv = divs[-1]
		timeSpan = contentDiv.find('span', attrs={'class':'ct'}).extract()
		links = contentDiv.findAll('a')
		extractedLinks = links[-4:]
		for link in extractedLinks: link.extract()

		#content
		content = contentDiv.get_text()
		hasImg = 0
		hasMultipleImg = 0
		hasLocation = 0
		if content.find('[组图]') != -1:
			hasMultipleImg = hasImg = 1
			content = content[: content.find('[组图]')]
		elif content.find('[图片]') != -1:
			hasImg = 1
			content = content[: content.find('[图片]')]
		if content.find('显示地图') != -1:
			hasLocation = 1
			content = content[: content.find('显示地图')]
		content = content.strip()
		links = contentDiv.findAll('a')

		#Location
		if hasLocation:
			locationLink = links[-sum([hasImg, hasMultipleImg, hasLocation])]
			latitude, longitude = getLocationInfo(locationLink)

		#resolve @info
		atLinks = links[:-sum([hasImg, hasMultipleImg, hasLocation])]
		for link in atLinks:
			name = link.get_text()
			#in case of other links in the weibo
			if name[0] != '@' or name.find(' ') != -1:
				continue
			name = name[1:]
			if name in frDict:
				dbWeiboAtList.append((mid, frDict[name]))
				if frDict[name] in staticsDict: 
					staticsDict[frDict[name]] += 1
				else: 
					staticsDict[frDict[name]] =1
			else:
				if name in unresolvedATDict:
					unresolvedATDict[name].append(mid)
				else:
					unresolvedATDict[name] = [mid]

		#Praise, Repost, Comment
		praiseCount, repostCount, commentCount = getCounts(extractedLinks)
		if praiseCount>0: midListWithPraise.append(mid)
		#if repostCount>0: midListWithRepost.append(mid)
		if commentCount>0: midListWithComment.append(mid)

		#Time
		timeInfo = timeSpan.get_text()
		timeString = timeInfo[:timeInfo.find('来自')].strip()
		postTime = timeFormatting(timeString)

		#mid, time, praiseCount, repostCount, commentCount, content, longitude, latitude, isRepost, originMid
		if isRepost:
			dbWeiboInfoList.append((mid, postTime, praiseCount, repostCount, commentCount, content, 0, 0, 1, originMid))
		elif hasLocation:
			dbWeiboInfoList.append((mid, postTime, praiseCount, repostCount, commentCount, content, longitude, latitude, 0, 0))
		else:
			dbWeiboInfoList.append((mid, postTime, praiseCount, repostCount, commentCount, content, 0, 0, 0, 0))
	SQL = "INSERT IGNORE INTO `posting` (`uid`, `mid`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbPostingList)

	SQL = "INSERT IGNORE INTO `weiboInfo` (`mid`, `time`, `praiseCount`, `repostCount`, `commentCount`, `content`, `longitude`, `latitude`, `isRepost`, `originMid`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	dbCur.executemany(SQL, dbWeiboInfoList)

	SQL = "INSERT IGNORE INTO `weiboAT` (`mid`, `uid`) VALUES (%s, %s)"
	dbCur.executemany(SQL, dbWeiboAtList)
	
	dbConn.commit()

	return [midListWithPraise, midListWithComment, unresolvedATDict, staticsDict, pageCount] if page == 1 else [midListWithPraise, midListWithComment, unresolvedATDict, staticsDict]

def getPageCount(pInfo):
	if not pInfo:
		return 1
	else:
		tmp = pInfo.get_text()
		rIndex = tmp.rfind('页')
		lIndex = tmp.rfind('1/')
		return int(tmp[lIndex+2:rIndex])

def getLocationInfo(locationLink):
	tmpUrl = locationLink['href']
	tmpUrl = tmpUrl[tmpUrl.find('center=')+7 :]
	tmpUrl = tmpUrl[: tmpUrl.find('&')]
	latitude, longitude = tmpUrl.split(',')
	latitude = float(latitude)
	longitude = float(longitude)
	return latitude, longitude

def getCounts(extractedLinks):
	praiseStr = extractedLinks[0].get_text()
	praiseCount = int(praiseStr[praiseStr.find('[')+1 : praiseStr.find(']')])
	repostStr = extractedLinks[1].get_text()
	repostCount = int(repostStr[repostStr.find('[')+1 : repostStr.find(']')])
	commentStr = extractedLinks[2].get_text()
	commentCount = int(commentStr[commentStr.find('[')+1 : commentStr.find(']')])
	return praiseCount, repostCount, commentCount


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