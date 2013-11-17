# -*- coding: utf-8 -*-
import logging
from time import sleep, time
from Queue import Queue, Empty
from DBUtils.PersistentDB import PersistentDB

from weiboCN import Fetcher, accountLimitedException, accountBannedException
from followList import followListProcessing
from fanList import fanListProcessing
from content import contentProcessing
from praise import praiseProcessing
from comment import commentProcessing
from resolve import weiboATResolving, commentResolving

MAX_WEIBO_PAGE = 20
MAX_COMMENT_PAGE = 5
MAX_PRAISE_PAGE = 5
FAN_COUNT_THRESHOLD = 10000
FOLLOW_COUNT_THRESHOLD = 10000
SWITCH_ACCOUNT_INTERVAL = 200
TIME_LIMIT_PER_REQ = 0.8
SLOT_SIZE = 10
def subworkerProcessing(tid, persistDB, todoQueue, resultQueue, assignedAccounts, pThread):
	dbConn = persistDB.connection()
	dbCur = dbConn.cursor()
	accountQueue = Queue()
	map(lambda x: accountQueue.put(x), assignedAccounts)
	account = accountQueue.get()
	logger = logging.getLogger("Thread%d" % tid)
	fetcher = Fetcher()
	recSpendTime = [TIME_LIMIT_PER_REQ]*SLOT_SIZE
	timeIndex = 0
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
				recSpendTime = [TIME_LIMIT_PER_REQ]*SLOT_SIZE
				timeIndex = 0
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

			switchAccountTimer += 1
			stTime = time()
			logger.debug("%r %r" % (account['user'], todo[:3]))
			cmd = todo[0]
			if cmd == "followList":
				uid = todo[1]
				page = todo[2]
				isNormalUID = True
				if page == 1:
					[followCount, pageCount, followDict] = followListProcessing(uid, page, fetcher, dbConn, dbCur)
					if followCount > FOLLOW_COUNT_THRESHOLD:
						logger.info('%d\'s followCount=%d, exceeds threshold' % (uid, followCount))
						isNormalUID = False
					else:
						for p in xrange(2, pageCount+1):
							todoQueue.put([cmd, uid, p])
				else:
					followDict = followListProcessing(uid, page, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, followDict, isNormalUID])

			elif cmd == "fanList":
				uid = todo[1]
				page = todo[2]
				isNormalUID = True
				if page == 1:
					[fanCount, pageCount, fanDict] = fanListProcessing(uid, page, fetcher, dbConn, dbCur)
					if fanCount > FAN_COUNT_THRESHOLD:
						logger.info('%d\'s fanCount=%d, exceeds threshold' % (uid, fanCount))
						isNormalUID = False
					else:
						for p in xrange(2, pageCount+1):
							todoQueue.put([cmd, uid, p])
				else:
					fanDict = fanListProcessing(uid, page, fetcher, dbConn, dbCur)
				resultQueue.put([cmd, fanDict, isNormalUID])

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
			avgTime = sum(recSpendTime)/SLOT_SIZE
			if avgTime < TIME_LIMIT_PER_REQ:
				logger.debug("Limiting rate for accout %s, delay for %.2fs" % (account['user'], (2*(TIME_LIMIT_PER_REQ-avgTime))))
				sleep(2*(TIME_LIMIT_PER_REQ-avgTime))
				recSpendTime[timeIndex] += 2*(TIME_LIMIT_PER_REQ-avgTime)
			timeIndex = (timeIndex+1)%SLOT_SIZE

		except accountLimitedException:
			todoQueue.put(todo)
			switchAccountTimer = -1
			logger.error("Account %s Limited, sleep 100s" % account['user'])
			sleep(100)

		except Exception, e:
			todoQueue.put(todo)
			logger.error("Exception: %r" % e)
		finally:
			if todo != None:
				todoQueue.task_done()
