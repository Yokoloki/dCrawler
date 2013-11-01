# -*- coding: utf-8 -*-
import logging
from time import sleep, time
from Queue import Queue
from DBUtils.PersistentDB import PersistentDB

from weiboCN import Fetcher, accountLimitedException, accountBannedException
from followList import followListProcessing
from fanList import fanListProcessing
from content import contentProcessing
from praise import praiseProcessing
from comment import commentProcessing
from resolve import weiboATResolving, commentResolving

MAX_WEIBO_PAGE = 20
MAX_COMMENT_PAGE = 10
MAX_PRAISE_PAGE = 10
SWITCH_ACCOUNT_INTERVAL = 800
RATE_LIMIT = 0.8
SLOT_SIZE = 10
def subworkerProcessing(tid, persistDB, todoQueue, resultQueue, assignedAccounts):
	dbConn = persistDB.connection()
	dbCur = dbConn.cursor()
	accountQueue = Queue()
	map(lambda x: accountQueue.put(x), assignedAccounts)
	account = accountQueue.get()
	logger = logging.getLogger("Thread%d" % tid)
	fetcher = Fetcher()
	recSpendTime = [0]*SLOT_SIZE
	timeIndex = 0
	while True:
		try:
			fetcher.login(account['user'], account['pwd'])
			break
		except accountBannedException:
			logger.error("account: %s banned....try to use other accounts" % account['user'])
			accountQueue.put(account)
			account = accountQueue.get()
			sleep(1)
	switchAccountTimer = 0
	while True:
		try:
			switchAccountTimer +=1
			if switchAccountTimer%SWITCH_ACCOUNT_INTERVAL == 0:
				switchAccountTimer = 0
				accountQueue.put(account)
				account = accountQueue.get()
				fetcher = Fetcher()
				recSpendTime = [0]*SLOT_SIZE
				timeIndex = 0
				while True:
					try:
						fetcher.login(account['user'], account['pwd'])
						break
					except accountBannedException:
						logger.error("account: %s banned....try to use other accounts" % account['user'])
						accountQueue.put(account)
						account = accountQueue.get()
						sleep(1)
			todo = todoQueue.get()
			stTime = time()
			logger.debug("%r %r" % (account['user'], todo))
			cmd = todo[0]
			if cmd == "followList":
				uid = todo[1]
				page = todo[2]
				if page == 1:
					[pageCount, followDict] = followListProcessing(uid, page, fetcher, dbConn, dbCur)
					for p in xrange(2, pageCount+1):
						todoQueue.put([cmd, uid, p])
				else:
					followDict = followListProcessing(uid, page, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, followDict])

			elif cmd == "fanList":
				uid = todo[1]
				page = todo[2]
				if page == 1:
					[pageCount, fanDict] = fanListProcessing(uid, page, fetcher, dbConn, dbCur)
					for p in xrange(2, pageCount+1):
						todoQueue.put([cmd, uid, p])
				else:
					fanDict = fanListProcessing(uid, page, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, fanDict])

			elif cmd == "content":
				uid = todo[1]
				page = todo[2]
				frDict = todo[3]
				if page == 1:
					[midListWithPraise, midListWithComment, unresolvedATDict, staticsDict, pageCount] = contentProcessing(uid, page, frDict, fetcher, dbConn, dbCur)
					for p in xrange(2, min(pageCount, MAX_WEIBO_PAGE)+1):
						todoQueue.put([cmd, uid, p, frDict])
				else:
					[midListWithPraise, midListWithComment, unresolvedATDict, staticsDict] = contentProcessing(uid, page, frDict, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, midListWithPraise, midListWithComment, unresolvedATDict, staticsDict])

			elif cmd == "resolveWeibo":
				name = todo[1]
				relatedMids = todo[2]
				[uid, atTimes] = weiboATResolving(name, relatedMids, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, name, uid, atTimes])

			elif cmd == "praise":
				mid = todo[1]
				page = todo[2]
				if page == 1:
					[pageCount, nameDict] = praiseProcessing(mid, page, fetcher, dbConn, dbCur)
					for p in xrange(2, min(pageCount, MAX_PRAISE_PAGE)+1):
						todoQueue.put([cmd, mid, p])
				else:
					nameDict = praiseProcessing(mid, page, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, nameDict])

			elif cmd == "comment":
				mid = todo[1]
				page = todo[2]
				frDict = todo[3]
				nameDict = todo[4]
				if page == 1:
					[unresolvedDict, staticsDict, pageCount] = commentProcessing(mid, page, frDict, nameDict, fetcher, dbConn, dbCur)
					for p in xrange(2, min(pageCount, MAX_COMMENT_PAGE)+1):
						todoQueue.put([cmd, mid, p, frDict, nameDict])
				else:
					[unresolvedDict, staticsDict] = commentProcessing(mid, page, frDict, nameDict, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, unresolvedDict, staticsDict])

			elif cmd == "resolveComment":
				name = todo[1]
				lists = todo[2]
				[uid, appTimes] = commentResolving(name, lists, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, name, uid, appTimes])
			#Rate Limit
			recSpendTime[timeIndex] = time() - stTime
			avgRate = sum(recSpendTime)/SLOT_SIZE
			if avgRate > 0.8:
				logger.info("Limiting rate for accout %s, dalay for %ds" % (account['user'], (2*(RATE_LIMIT-avgRate))))
				sleep(2*(RATE_LIMIT-avgRate))
				recSpendTime[timeIndex] += 2*(RATE_LIMIT-avgRate)
			timeIndex = (timeIndex+1)%SLOT_SIZE

		except accountLimitedException:
			todoQueue.put(todo)
			#force switch account
			switchAccountTimer = -1
			logger.error("AccountLimited, sleep 300s")
			sleep(300)

		except Exception, e:
			todoQueue.put(todo)
			logger.error("Exception: %r" % e)
		finally:
			todoQueue.task_done()
