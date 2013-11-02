# -*- coding: utf-8 -*-
import sys
import rpyc
import json
import logging, logging.config, logging.handlers
import MySQLdb
from Queue import Queue
from threading import Thread
from operator import itemgetter
from rpyc.utils.server import ThreadedServer
from rpyc.utils.registry import TCPRegistryClient
from DBUtils.PersistentDB import PersistentDB
from time import sleep
from subworker import subworkerProcessing
from copy import deepcopy

reload(sys)
sys.setdefaultencoding("utf-8")

MAX_FRLIST_NUM = 100
class CrawlerService(rpyc.Service):
	class exposed_Crawler(object):
		def __init__(self, serverID, threadCount, accountList, fetchNewJobCB):
			#Import configs
			confFile = file("/home/loki/dCrawler/slave/config.json")
			config = json.load(confFile)
			confFile.close()
			#Init variables
			self.id = deepcopy(serverID)
			self.threadCount = deepcopy(threadCount)
			self.accountList = deepcopy(accountList)
			self.callback = fetchNewJobCB
			self.todoQueue = Queue()
			self.resultQueue = Queue()
			#Init DBpool
			dbconfig = config['db']
			self.persistDB = PersistentDB(MySQLdb, host=dbconfig['host'], user=dbconfig['user'], passwd=dbconfig['passwd'], db=dbconfig['name'], charset='utf8')
			#Init Logger
			logconfig = config['log']
			logging.config.dictConfig(logconfig)
			self.logger = logging.getLogger("main")
			self.logger.info("accounts to use: %r" % self.accountList)
			#Start working thread
			self.thread = Thread(target = self.work)
			self.thread.start()

		def work(self):
			self.logger.info("start to work")
			#Init local variables
			job = None
			frList = []
			t = 0
			dbConn = self.persistDB.connection()
			dbCur = dbConn.cursor()
			SQL = "INSERT IGNORE INTO `crawledUID` (`uid`) VALUES (%d)"
			numAcPThread = len(self.accountList) / self.threadCount
			#Init threads for work
			for _ in xrange(self.threadCount):
				assignedAccounts =  map(lambda x: self.accountList.pop(), xrange(numAcPThread))
				thread = Thread(target = subworkerProcessing, args=(_+1, self.persistDB, self.todoQueue, self.resultQueue, assignedAccounts, ))
				thread.daemon = True
				thread.start()
			#Start the working loop
			while True:
				#CB & Fetch Jobs
				job = self.callback(self.id, job, frList)
				if job == None:
					self.logger.error("Jobs are currently unavailable, try again in 1s")
					sleep(1)
					continue
				job = deepcopy(job)
				frList = self.crawl(job)
				#Update DB when finish
				dbCur.execute(SQL % job)
				dbConn.commit()
				self.logger.info("complete job %r" % job)

		def crawl(self, uid):
			#frDict: nickName->uid
			#staticsDict: uid->calledTimes
			self.logger.info('%d - crawlFrList' % uid)
			frDict = self.crawlFrList(uid)
			self.logger.info('%d - crawlContent' % uid)
			[midListWithPraise, midListWithComment, unresolvedATDict, staticsDict] = self.crawlContent(uid, frDict)
			#Resolve @Names, update the staticsDict, and return the name->uid dict
			self.logger.info('%d - resolveWeiboNames' % uid)
			nameDict = self.resolveWeiboNames(unresolvedATDict, staticsDict)
			self.logger.info('%d - crawlPraise' % uid)
			nameDict = self.crawlPraise(midListWithPraise, nameDict)
			self.logger.info('%d - crawlComment' % uid)
			unresolvedDict = self.crawlComment(midListWithComment, frDict, nameDict, staticsDict)
			#Resolve names appears in comments, update the staticsDict and return the name->uid dict
			self.logger.info('%d - resolveCommentNames' % uid)
			self.resolveCommentNames(unresolvedDict, staticsDict)
			frList = self.getPotentialFrs(staticsDict)
			return frList

		def crawlFrList(self, uid):
			fanListJob = ['fanList', uid, 1]
			followListJob = ['followList', uid, 1]
			self.todoQueue.put(fanListJob)
			self.todoQueue.put(followListJob)
			self.todoQueue.join()
			#Dict: NickName->UID
			followDict = {}
			fanDict = {}
			frDict = {}
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				if result[0] == 'followList':
					followDict.update(result[1])
				else:
					fanDict.update(result[1])
			friendNameList = list(set(followDict) & set(fanDict))
			for name in friendNameList:
				frDict[name] = followDict[name]
			return frDict

		def crawlContent(self, uid, frDict):
			self.todoQueue.put(['content', uid, 1, frDict])
			self.todoQueue.join()

			midListWithPraise = []
			midListWithComment = []
			unresolvedATDict = {}
			staticsDict = {}
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				midListWithPraise += result[1]
				midListWithComment += result[2]
				tUnresolvedATDict = result[3]
				tStaticsDict = result[4]
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
			return [midListWithPraise, midListWithComment, unresolvedATDict, staticsDict]

		def resolveWeiboNames(self, unresolvedATDict, staticsDict):
			for name, mids in unresolvedATDict.iteritems():
				self.todoQueue.put(['resolveWeibo', name, mids])
			self.todoQueue.join()
			nameDict = {}
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				name = result[1]
				uid = result[2]
				atTimes = result[3]
				nameDict[name] = uid
				if uid in staticsDict:
					staticsDict[uid] += atTimes
				else:
					staticsDict[uid] = atTimes
			return nameDict

		def crawlPraise(self, midList, nameDict):
			for mid in midList:
				self.todoQueue.put(['praise', mid, 1])
			self.todoQueue.join()
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				for name, uid in result[1].iteritems():
					if not name in nameDict:
						nameDict[name] = uid
			return nameDict

		def crawlComment(self, midList, frDict, nameDict, staticsDict):
			for mid in midList:
				self.todoQueue.put(['comment', mid, 1, frDict, nameDict])
			self.todoQueue.join()
			unresolvedDict = {}
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
			return unresolvedDict

		def resolveCommentNames(self, unresolvedDict, staticsDict):
			for name, lists in unresolvedDict.iteritems():
				self.todoQueue.put(['resolveComment', name, lists])
			self.todoQueue.join()
			nameDict = {}
			while not self.resultQueue.empty():
				result = self.resultQueue.get_nowait()
				name = result[1]
				uid = result[2]
				appTimes = result[3]
				nameDict[name] = uid
				if uid in staticsDict:
					staticsDict[uid] += appTimes
				else:
					staticsDict[uid] = appTimes
			return nameDict

		def getPotentialFrs(self, staticsDict):
			if staticsDict.get(-1):
				staticsDict.pop(-1)
			sortedList = sorted(staticsDict.iteritems(), key=itemgetter(1), reverse=True)
			frList = map(lambda x: x[0], sortedList[: min(len(sortedList), MAX_FRLIST_NUM)])
			return frList

if __name__ =="__main__":
	#s = ThreadedServer(CrawlerService, port=18000, registrar=TCPRegistryClient("172.18.216.161"), logger = logging.getLogger())
	s = ThreadedServer(CrawlerService, port=18000)
	s.start()