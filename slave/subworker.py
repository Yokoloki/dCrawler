# -*- coding: utf-8 -*-
import logging
from time import sleep, time
from Queue import Queue, Empty
from DBUtils.PersistentDB import PersistentDB
from random import random

from weiboCN import Fetcher, accountLimitedException, accountFreezedException, accountBannedException, accountErrorException
from followList import followListProcessing
from fanList import fanListProcessing
from content import contentProcessing, contentUpdating
from praise import praiseProcessing
from comment import commentProcessing
from resolve import weiboATResolving, commentResolving

MAX_WEIBO_PAGE = 20
MAX_COMMENT_PAGE = 5
MAX_PRAISE_PAGE = 5
FAN_COUNT_THRESHOLD = 10000
FOLLOW_COUNT_THRESHOLD = 10000
SWITCH_ACCOUNT_INTERVAL = 200

def subworkerProcessing(tid, persistDB, todoQueue, resultQueue, assignedAccounts, pThread):
	dbConn = persistDB.connection()
	dbCur = dbConn.cursor()
	accountQueue = Queue()
	map(lambda x: accountQueue.put(x), assignedAccounts)
	account = accountQueue.get()
	logger = logging.getLogger("Thread%d" % tid)
	fetcher = Fetcher()
	while pThread.isAlive():
		try:
			fetcher.login(account['user'], account['pwd'])
			break
		except accountBannedException:
			logger.error("account: %s banned....try to use other accounts" % account['user'])
			accountQueue.put(account)
			account = accountQueue.get()
			sleep(1)
	switchAccountTimer = 0
	while pThread.isAlive():
		try:
			switchAccountTimer +=1
			if switchAccountTimer%SWITCH_ACCOUNT_INTERVAL == 0:
				switchAccountTimer = 0
				accountQueue.put(account)
				account = accountQueue.get()
				fetcher = Fetcher()
				while pThread.isAlive():
					try:
						fetcher.login(account['user'], account['pwd'])
						break
					except accountBannedException:
						logger.error("account: %s banned....try to use other accounts" % account['user'])
						accountQueue.put(account)
						account = accountQueue.get()
						sleep(1)
			try:
				todo = todoQueue.get(True, 3)
			except Empty:
				todo = None
				continue
			sleep(random())

			switchAccountTimer += 1
			stTime = time()
			logger.debug("%r %r" % (account['user'], todo[:3]))
			cmd = todo[0]

			if cmd == "followList":
				uid = todo[1]
				page = todo[2]
				isNormalUID = True
				if page == 1:
					try:
						[followCount, pageCount, followDict, dbFollowingList, dbInfoList] = followListProcessing(uid, page, fetcher)
						if followCount > FOLLOW_COUNT_THRESHOLD:
							logger.info('%d\'s followCount=%d, exceeds threshold' % (uid, followCount))
							isNormalUID = False
						else:
							for p in xrange(2, pageCount+1):
								todoQueue.put([cmd, uid, p])
					except accountErrorException:
						isNormalUID = False
						fanDict = {}
						dbFollowingList = []
						dbInfoList = []
						logger.error("Account %s Error & cannot access" % account['user'])

				else:
					[followDict, dbFollowingList, dbInfoList] = followListProcessing(uid, page, fetcher)
				resultQueue.put([cmd, followDict, isNormalUID, dbFollowingList, dbInfoList])

			elif cmd == "fanList":
				uid = todo[1]
				page = todo[2]
				isNormalUID = True
				if page == 1:
					try:
						[fanCount, pageCount, fanDict, dbFollowingList, dbInfoList] = fanListProcessing(uid, page, fetcher)
						if fanCount > FAN_COUNT_THRESHOLD:
							logger.info('%d\'s fanCount=%d, exceeds threshold' % (uid, fanCount))
							isNormalUID = False
						else:
							for p in xrange(2, pageCount+1):
								todoQueue.put([cmd, uid, p])
					except accountErrorException:
						isNormalUID = False
						fanDict = {}
						dbFollowingList = []
						dbInfoList = []
						logger.error("Account %s Error & cannot access" % account['user'])

				else:
					[fanDict, dbFollowingList, dbInfoList] = fanListProcessing(uid, page, fetcher)
				resultQueue.put([cmd, fanDict, isNormalUID, dbFollowingList, dbInfoList])

			elif cmd == "content":
				uid = todo[1]
				page = todo[2]
				frDict = todo[3]
				if page == 1:
					[pageCount, midListWithPraise, midListWithComment, unresolvedATDict, staticsDict, dbPostingList, dbWeiboInfoList, dbWeiboAtList] = contentProcessing(uid, page, frDict, fetcher)
					for p in xrange(2, min(pageCount, MAX_WEIBO_PAGE)+1):
						todoQueue.put([cmd, uid, p, frDict])
				else:
					[midListWithPraise, midListWithComment, unresolvedATDict, staticsDict, dbPostingList, dbWeiboInfoList, dbWeiboAtList] = contentProcessing(uid, page, frDict, fetcher)
				resultQueue.put([cmd, midListWithPraise, midListWithComment, unresolvedATDict, staticsDict, dbPostingList, dbWeiboInfoList, dbWeiboAtList])
			
			elif cmd == "ucontent":
				uid = todo[1]
				page = todo[2]
				frDict = todo[3]
				if page == 1:
					[pageCount, repostWeibo] = contentUpdating(uid, page, frDict, fetcher)
					for p in xrange(2, min(pageCount, MAX_WEIBO_PAGE)+1):
						todoQueue.put([cmd, uid, p, frDict])
				else:
					repostWeibo = contentUpdating(uid, page, frDict, fetcher)
				resultQueue.put([cmd, repostWeibo])

			elif cmd == "resolveWeibo":
				name = todo[1]
				relatedMids = todo[2]
				[uid, atTimes, dbWeiboAtList, dbUserInfo] = weiboATResolving(name, relatedMids, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, name, uid, atTimes, dbWeiboAtList, dbUserInfo])

			elif cmd == "praise":
				mid = todo[1]
				page = todo[2]
				if page == 1:
					[pageCount, nameDict, dbPraisedByList] = praiseProcessing(mid, page, fetcher)
					for p in xrange(2, min(pageCount, MAX_PRAISE_PAGE)+1):
						todoQueue.put([cmd, mid, p])
				else:
					[nameDict, dbPraisedByList] = praiseProcessing(mid, page, fetcher)
				resultQueue.put([cmd, nameDict, dbPraisedByList])

			elif cmd == "comment":
				mid = todo[1]
				page = todo[2]
				frDict = todo[3]
				nameDict = todo[4]
				if page == 1:
					[pageCount, unresolvedDict, staticsDict, dbCommentOfList, dbCommentingList, dbCommentList, dbCommentATList] = commentProcessing(mid, page, frDict, nameDict, fetcher)
					for p in xrange(2, min(pageCount, MAX_COMMENT_PAGE)+1):
						todoQueue.put([cmd, mid, p, frDict, nameDict])
				else:
					[unresolvedDict, staticsDict, dbCommentOfList, dbCommentingList, dbCommentList, dbCommentATList] = commentProcessing(mid, page, frDict, nameDict, fetcher)
				resultQueue.put([cmd, unresolvedDict, staticsDict, dbCommentOfList, dbCommentingList, dbCommentList, dbCommentATList])

			elif cmd == "resolveComment":
				name = todo[1]
				lists = todo[2]
				[uid, appTimes, dbCommentingList, dbCommentReplyList, dbCommentATList, dbUserInfo] = commentResolving(name, lists, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, name, uid, appTimes, dbCommentingList, dbCommentReplyList, dbCommentATList, dbUserInfo])

		except accountLimitedException:
			todoQueue.put(todo)
			switchAccountTimer = -1
			logger.error("Account %s Limited, sleep 60s" % account['user'])
			sleep(60)

		except accountFreezedException:
			todoQueue.put(todo)
			switchAccountTimer = -1
			logger.error("Account %s Freezed" % account['user'])
			sleep(1)

		except Exception, e:
			todoQueue.put(todo)
			logger.error("Exception: %r when doing %r" % (e, todo))
			sleep(60)
			
		finally:
			if todo != None:
				todoQueue.task_done()
