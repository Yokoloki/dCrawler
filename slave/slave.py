# -*- coding: utf-8 -*-
import sys
import rpyc
import json
import logging, logging.config, logging.handlers
import MySQLdb
from Queue import Queue
from threading import Thread, currentThread
from operator import itemgetter
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import TCPRegistryClient
from DBUtils.PersistentDB import PersistentDB
from time import sleep, time, localtime, strftime
from subworker import subworkerProcessing
from copy import deepcopy

reload(sys)
sys.setdefaultencoding("utf-8")
JOIN_TIMEOUT_PERROUNT = 300

class timeoutQueue(Queue):
	def join_with_timeout(self, timeout):
		self.all_tasks_done.acquire()
		try:
			endTime = time() + timeout
			while self.unfinished_tasks:
				remaining = endTime - time()
				if remaining <= 0.0:
					raise JoinTimeout
				self.all_tasks_done.wait(remaining)
		finally:
			self.all_tasks_done.release()
class JoinTimeout(Exception):
	pass

MAX_FRLIST_NUM = 200
class CrawlerService(rpyc.Service):
	class exposed_Crawler(object):
		def __init__(self, serverID, threadCount, dbConfig, accountList, fetchNewJobCB):
			#Init variables
			self.id = deepcopy(serverID)
			self.threadCount = deepcopy(threadCount)
			self.dbConfig = deepcopy(dbConfig)
			self.accountList = deepcopy(accountList)
			self.callback = fetchNewJobCB
			self.todoQueue = timeoutQueue()
			self.resultQueue = Queue()
			self.accountQueue = Queue()
			map(lambda x: self.accountQueue.put(x), accountList)
			#Init DBpool
			self.persistDB = PersistentDB(MySQLdb, host=self.dbConfig['host'], user=self.dbConfig['user'], passwd=self.dbConfig['passwd'], db=self.dbConfig['name'], charset='utf8')
			self.dbConn = self.persistDB.connection()
			self.dbCur = self.dbConn.cursor()
			#Init Logger
			self.logger = logging.getLogger("main")
			self.logger.info("accounts to use: %r" % self.accountList)
			#Start working thread
			self.thread = Thread(target = self.work, args=(currentThread(), ))
			self.thread.start()
			#Init threads for work
			numAcPThread = len(self.accountList) / self.threadCount
			for _ in xrange(self.threadCount):
				assignedAccounts =  map(lambda x: self.accountList.pop(), xrange(numAcPThread))
				thread = Thread(target = subworkerProcessing, args=(_+1, self.persistDB, self.todoQueue, self.resultQueue, assignedAccounts, currentThread(), ))
				thread.start()

		def work(self, pThread):
			self.logger.info("start to work")
			#Init local variables
			job = None
			finished = True
			frList = []
			t = 0
			SQL1 = "INSERT INTO `crawledUID` (`uid`, `lastUpdate`) VALUES (\'%d\', \'%s\') ON DUPLICATE KEY UPDATE `lastUpdate`=values(`lastUpdate`)"
			SQL2 = "INSERT IGNORE INTO `cpUID` (`uid`) VALUES (\'%d\')"
			#Start the working loop
			while pThread.isAlive():
				try:
					job = self.callback(self.id, job, finished, frList)
					if job == None:
						self.logger.error("Jobs are currently unavailable, try again in 2s")
						sleep(2)
						continue
					job = deepcopy(job)
					frList = self.crawl(job)
					finished = True
					#Update DB when finish
					self.dbCur.execute(SQL1 % (job, strftime("%Y-%m-%d %H:%M:%S", localtime())))
					if frList == None:
						self.dbCur.execute(SQL2 % job)
					self.dbConn.commit()
					self.logger.info("complete job %r" % job)
				except JoinTimeout, e:
					self.logger.error('JoinTimeoutException: %r' % e)
					frList = None
					finished = False
					self.todoQueue = timeoutQueue()
					self.resultQueue = Queue()
				except Exception, e:
					self.logger.error('Exception: %r' % e)

		def crawl(self, uid):
			#frDict: nickName->uid
			#staticsDict: uid->calledTimes
			if uid == -1: return None
			frDict = self.crawlFrList(uid)
			if frDict == None:
				return None
			[midListWithPraise, midListWithComment, unresolvedATDict, staticsDict] = self.crawlContent(uid, frDict)
			#Resolve @Names, update the staticsDict, and return the name->uid dict
			nameDict = self.resolveWeiboNames(unresolvedATDict, staticsDict)
			nameDict = self.crawlPraise(midListWithPraise, nameDict)
			unresolvedDict = self.crawlComment(midListWithComment, frDict, nameDict, staticsDict)
			#Resolve names appears in comments, update the staticsDict and return the name->uid dict
			self.resolveCommentNames(unresolvedDict, staticsDict)
			frList = self.getPotentialFrs(staticsDict)
			return frList

		def crawlFrList(self, uid):
			self.logger.info('crawling FrList of %d' % uid)
			fanListJob = ['fanList', uid, 1]
			followListJob = ['followList', uid, 1]
			self.todoQueue.put(fanListJob)
			self.todoQueue.put(followListJob)
			self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT)
			#Dict: NickName->UID
			followDict = {}
			fanDict = {}
			frDict = {}
			isNormalID = True
			dbFollowingList = []
			dbInfoList = []
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				isNormalID &= result[2]
				dbFollowingList += result[3]
				dbInfoList += result[4]
				if result[0] == 'followList':
					followDict.update(result[1])
				else:
					fanDict.update(result[1])
			if not isNormalID:
				return None
			dbFollowingList = list(set(dbFollowingList))
			dbInfoList = list(set(dbInfoList))

			SQL = "INSERT IGNORE INTO `following` (`followerID`, `followedID`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbFollowingList)
			SQL = "INSERT IGNORE INTO `userInfo` (`uid`, `name`, `imgUrl`) VALUES (%s, %s, %s)"
			self.dbCur.executemany(SQL, dbInfoList)
			self.dbConn.commit()
			
			friendNameList = list(set(followDict) & set(fanDict))
			for name in friendNameList:
				frDict[name] = followDict[name]
			return frDict

		def crawlContent(self, uid, frDict):
			self.logger.info('crawling weibo content of %d' % uid)
			self.todoQueue.put(['content', uid, 1, frDict])
			self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT)

			midListWithPraise = []
			midListWithComment = []
			unresolvedATDict = {}
			staticsDict = {}
			dbPostingList = []
			dbWeiboInfoList = []
			dbWeiboAtList = []
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				midListWithPraise += result[1]
				midListWithComment += result[2]
				tUnresolvedATDict = result[3]
				tStaticsDict = result[4]
				dbPostingList += result[5]
				dbWeiboInfoList += result[6]
				dbWeiboAtList += result[7]
				for k, v in tUnresolvedATDict.iteritems():
					if k in unresolvedATDict:
						unresolvedATDict[k] += v
					else:
						unresolvedATDict[k] = v
				for k, v in tStaticsDict.iteritems():
					if k in staticsDict:
						staticsDict[k] += v
					else:
						staticsDict[k] = v
			dbPostingList = list(set(dbPostingList))
			dbWeiboInfoList = list(set(dbWeiboInfoList))
			dbWeiboAtList = list(set(dbWeiboAtList))

			SQL = "INSERT IGNORE INTO `posting` (`uid`, `mid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbPostingList)
			SQL = "INSERT IGNORE INTO `weiboInfo` (`mid`, `time`, `praiseCount`, `repostCount`, `commentCount`, `content`, `longitude`, `latitude`, `isRepost`, `originMid`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
			self.dbCur.executemany(SQL, dbWeiboInfoList)
			SQL = "INSERT IGNORE INTO `weiboAT` (`mid`, `uid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbWeiboAtList)
			self.dbConn.commit()

			return [midListWithPraise, midListWithComment, unresolvedATDict, staticsDict]

		def resolveWeiboNames(self, unresolvedATDict, staticsDict):
			self.logger.info('%d names to resolve for weibo AT' % len(staticsDict))
			for name, mids in unresolvedATDict.iteritems():
				self.todoQueue.put(['resolveWeibo', name, mids])
			self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT)
			nameDict = {}
			dbWeiboAtList = []
			dbUserInfo = []
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				name = result[1]
				uid = result[2]
				atTimes = result[3]
				dbWeiboAtList += result[4]
				dbUserInfo += result[5]
				nameDict[name] = uid
				if uid in staticsDict:
					staticsDict[uid] += atTimes
				else:
					staticsDict[uid] = atTimes
			dbWeiboAtList = list(set(dbWeiboAtList))
			dbUserInfo = list(set(dbUserInfo))
			SQL = "INSERT IGNORE INTO `weiboAT` (`mid`, `uid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbWeiboAtList)
			SQL = "INSERT IGNORE INTO `userInfo` (`uid`, `name`, `imgUrl`) VALUES (%s, %s, %s)"
			self.dbCur.executemany(SQL, dbUserInfo)
			self.dbConn.commit()
			return nameDict

		def crawlPraise(self, midList, nameDict):
			self.logger.info('%d weibos\' parise to crawl' % len(midList))
			for mid in midList:
				self.todoQueue.put(['praise', mid, 1])
			self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT)
			dbPraisedByList = []
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				dbPraisedByList += result[2]
				for name, uid in result[1].iteritems():
					if not name in nameDict:
						nameDict[name] = uid
			dbPraisedByList = list(set(dbPraisedByList))
			SQL = "INSERT IGNORE INTO `praisedBy` (`mid`, `uid`, `time`) VALUES (%s, %s, %s)"
			self.dbCur.executemany(SQL, dbPraisedByList)
			self.dbConn.commit()

			return nameDict

		def crawlComment(self, midList, frDict, nameDict, staticsDict):
			self.logger.info('%d weibos\' comment to crawl' % len(midList))
			for mid in midList:
				self.todoQueue.put(['comment', mid, 1, frDict, nameDict])
			self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT)
			unresolvedDict = {}
			dbCommentOfList = []
			dbCommentingList = []
			dbCommentList = []
			dbCommentATList = []
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				for name, lists in result[1].iteritems():
					if name in unresolvedDict:
						unresolvedDict[name][0] += lists[0]
						unresolvedDict[name][1] += lists[1]
						unresolvedDict[name][2] += lists[2]
					else:
						unresolvedDict[name] = lists
				tStaticsDict = result[2]
				for uid, times in tStaticsDict.iteritems():
					if uid in staticsDict:
						staticsDict[uid] += times
					else:
						staticsDict[uid] = times
				dbCommentOfList += result[3]
				dbCommentingList += result[4]
				dbCommentList += result[5]
				dbCommentATList += result[6]
			dbCommentOfList = list(set(dbCommentOfList))
			dbCommentingList = list(set(dbCommentingList))
			dbCommentList = list(set(dbCommentList))
			dbCommentATList = list(set(dbCommentATList))

			SQL = "INSERT IGNORE INTO `commentOf` (`cid`, `mid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbCommentOfList)
			SQL = "INSERT IGNORE INTO `commenting` (`uid`, `cid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbCommentingList)
			SQL = "INSERT IGNORE INTO `commentInfo` (`cid`, `time`, `content`, `isReply`, `replyTo`) VALUES (%s, %s, %s, %s, %s)"
			self.dbCur.executemany(SQL, dbCommentList)
			SQL = "INSERT IGNORE INTO `commentAT` (`cid`, `uid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbCommentATList)
			self.dbConn.commit()
			return unresolvedDict

		def resolveCommentNames(self, unresolvedDict, staticsDict):
			self.logger.info('%d names to resolve for comment' % len(staticsDict))
			for name, lists in unresolvedDict.iteritems():
				self.todoQueue.put(['resolveComment', name, lists])
			self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT*2)
			nameDict = {}
			dbCommentingList = []
			dbCommentReplyList = [] 
			dbCommentATList = []
			dbUserInfo = []
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				name = result[1]
				uid = result[2]
				appTimes = result[3]
				dbCommentingList += result[4]
				dbCommentReplyList += result[5] 
				dbCommentATList += result[6]
				dbUserInfo += result[7]
				nameDict[name] = uid
				if uid in staticsDict:
					staticsDict[uid] += appTimes
				else:
					staticsDict[uid] = appTimes

			dbCommentingList = list(set(dbCommentingList))
			dbCommentReplyList = list(set(dbCommentReplyList))
			dbCommentATList = list(set(dbCommentATList))
			dbUserInfo = list(set(dbUserInfo))

			SQL = "INSERT IGNORE INTO `commenting` (`uid`, `cid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbCommentingList)
			SQL = "UPDATE `commentInfo` SET `replyTo`=%s WHERE `cid`=%s"
			self.dbCur.executemany(SQL, dbCommentReplyList)
			SQL = "INSERT IGNORE INTO `commentAT` (`cid`, `uid`) VALUES (%s, %s)"
			self.dbCur.executemany(SQL, dbCommentATList)
			SQL = "INSERT IGNORE INTO `userInfo` (`uid`, `name`, `imgUrl`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `name`=values(`name`), `imgUrl`=values(`imgUrl`)"
			self.dbCur.executemany(SQL, dbUserInfo)

			self.dbConn.commit()
			return nameDict

		def getPotentialFrs(self, staticsDict):
			if staticsDict.get(-1):
				staticsDict.pop(-1)
			sortedList = sorted(staticsDict.iteritems(), key=itemgetter(1), reverse=True)
			frList = map(lambda x: x[0], sortedList[: min(len(sortedList), MAX_FRLIST_NUM)])
			return frList

if __name__ =="__main__":
	confFile = file("/home/loki/dCrawler/slave/config.json")
	config = json.load(confFile)
	confFile.close()
	logging.config.dictConfig(config['log'])
	s = ThreadedServer(CrawlerService, port=18000, protocol_config={"allow_pickle":True})
	s.start()