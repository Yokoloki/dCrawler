# -*- coding: utf-8 -*-
import sys
import rpyc
import json
import MySQLdb
import random
import logging, logging.config, logging.handlers
from operator import itemgetter
from Queue import Queue, Empty
from time import time, localtime, sleep
from copy import deepcopy

reload(sys)
sys.setdefaultencoding("utf-8")

MAXNUM_TOCRAWL_EACHROUND = 100
JOIN_TIMEOUT_PERROUNT = 2000

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

class Scheduler:
	def __init__(self):
		#Import weibo accounts from json
		#[{'user':'xxx', 'psw':'xxx'}, ...]'
		acFile = file("accounts.json")
		self.accounts = json.load(acFile)
		random.shuffle(self.accounts)
		acFile.close()
		#Import other configs(DB)
		confFile = file("config.json")
		self.config = json.load(confFile)
		confFile.close()

		logging.config.dictConfig(self.config['log'])
		self.logger = logging.getLogger()

		self.dbConfig = self.config['db']
		self.threadCount = self.config['threadCount']
		self.serverPool = self.config['servers']
		#Init variables
		self.todoQueue = timeoutQueue()
		self.doneQueue = Queue()
		self.crawledSet = set()
		self.crawledSet.add(-1)
		self.numCrawled = 0
		self.dbConn = MySQLdb.connect(host=self.dbConfig['host'], user=self.dbConfig['user'], passwd=self.dbConfig['passwd'], db=self.dbConfig['name'], charset='utf8')
		self.dbCur = self.dbConn.cursor()
		#Init workers
		self.connList = []
		self.threadList = []
		self.crawlerList = []
		self.initSlaveNodes()


	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		for thread in self.threadList:
			thread.stop()
		for conn in self.connList:
			conn.close()
		self.dbCur.close()
		self.dbConn.close()
		self.logger.info("%d user data are crawled in this execution" % self.numCrawled)
		return

	def initSlaveNodes(self):
		self.logger.info('Connecting with remote servers...')
		for thread in self.threadList:
			thread.stop()
		for conn in self.connList:
			conn.close()
		sleep(10)
		self.connList = []
		self.threadList = []
		self.crawlerList = []
		random.shuffle(self.accounts)
		for serverID, server in enumerate(self.serverPool):
			conn = rpyc.connect(server[0], server[1], config={"allow_pickle":True})
			bgThread = rpyc.BgServingThread(conn)
			accountList = self.accounts[serverID*len(self.accounts)/len(self.serverPool):(serverID+1)*len(self.accounts)/len(self.serverPool)]
			crawler = conn.root.Crawler(serverID, self.threadCount, self.dbConfig, accountList, self.fetchNewJob)
			self.connList.append(conn)
			self.threadList.append(bgThread)
			self.crawlerList.append(crawler)

	def retrieveFromDB(self):
		#Get users already crawled
		SQL = 'SELECT `uid` FROM `crawledUID` WHERE 1'
		self.dbCur.execute(SQL)
		result = self.dbCur.fetchall()
		map(lambda x: self.crawledSet.add(x[0]), result)
		#Get users that are friends of crawled users but not crawled
		SQL = 'SELECT T1.`followedID` FROM `crawledUID` as U, `following` as T1, `following` as T2 WHERE '
		SQL += 'T1.`followerID`=T2.`followedID` and T1.`followedID`=T2.`followerID` and T1.`followerID`=U.`uid`'
		self.dbCur.execute(SQL)
		result = self.dbCur.fetchall()
		toCrawlDict = {}
		for uid in result:
			if uid[0] not in self.crawledSet:
				toCrawlDict[uid[0]] = toCrawlDict[uid[0]]+1 if toCrawlDict.get(uid[0]) else 1
		return toCrawlDict

	def getPriorUIDs(self):
		SQL = "SELECT `uid` FROM `priorUID` WHERE 1"
		self.dbCur.execute(SQL)
		results = self.dbCur.fetchall()
		uidList = []
		for item in results:
			if item[0] not in self.crawledSet:
				uidList.append(item[0])
		SQL = "TRUNCATE `priorUID`"
		self.dbCur.execute(SQL)
		return uidList

	def fetchNewJob(self, serverID, prevJob, finished, frList):
		if prevJob != None:
			self.logger.debug("Server%d return the result of previous Job" % serverID)
			self.todoQueue.task_done()
			if finished:
				self.doneQueue.put({'sid':deepcopy(serverID), 'uid':deepcopy(prevJob), 'frList':deepcopy(frList)})
		self.logger.debug("Server%d trying to get a new job" % serverID)
		try:
			newJob = self.todoQueue.get(True, 5)
		except Empty:
			newJob = None
		return newJob

	def work(self):
		#Retrive info of last execution from DB
		stTime = localtime()
		self.logger.info('Retrieving information from database...')
		toCrawlDict = self.retrieveFromDB()
		canceled = False
		reInit = False
		#setupWorkersNodes & let them block in todoQueue.get()
		while not canceled:
			while not self.doneQueue.empty():
				doneJobInfo = self.doneQueue.get_nowait()
				if toCrawlDict.get(doneJobInfo['uid']):
					toCrawlDict.pop(doneJobInfo['uid'])
				self.crawledSet.add(doneJobInfo['uid'])
				self.logger.debug("%d crawled" % doneJobInfo['uid'])
				if doneJobInfo['frList']:
					for fr in doneJobInfo['frList']:
						if fr not in self.crawledSet:
							toCrawlDict[fr] = toCrawlDict[fr]+1 if toCrawlDict.get(fr) else 1
			if reInit:
				self.logger.info('Reinit slave nodes')
				self.initSlaveNodes()
			priorUIDList = self.getPriorUIDs()
			map(lambda x: self.todoQueue.put(x), priorUIDList)
			if priorUIDList == []:
				sortedList = sorted(toCrawlDict.iteritems(), key=itemgetter(1), reverse=True)
				#Only choose those more valuable user if more than x uid to add
				toCrawlList = map(lambda x:x[0], sortedList[:min(MAXNUM_TOCRAWL_EACHROUND, len(sortedList))])
				for i in xrange(len(toCrawlList)):
					self.todoQueue.put(toCrawlList[i])
				self.logger.info("Uids to crawl: %r" % toCrawlList)
			else:
				self.logger.info("Uids to crawl: %r" % priorUIDList)
			#Wait until this round of jobs done
			sTime = time()
			try:
				self.todoQueue.join_with_timeout(JOIN_TIMEOUT_PERROUNT)
			except KeyboardInterrupt:
				canceled = True
			except JoinTimeout:
				reInit = True
				self.logger.error('todoQueue join timeout')
			eTime = time()
			self.numCrawled += self.doneQueue.qsize()
			self.logger.info("%d users crawled in %fs, %f/s" % (
				self.doneQueue.qsize(), eTime-sTime,
				(self.doneQueue.qsize()/(eTime-sTime))))
			self.logger.info("%d users are crawled since %d-%d-%d %d:%d" % (self.numCrawled, stTime[0], stTime[1], stTime[2], stTime[3], stTime[4]))
		self.summary()

	def summary(self):
		pass

if __name__ == "__main__":
	with Scheduler() as ins:
		ins.work()