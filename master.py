# -*- coding: utf-8 -*-
import rpyc
import json
import MySQLdb
import logging, logging.config, logging.handlers
from operator import itemgetter
from Queue import Queue, Empty
from time import time

MAXNUM_TOCRAWL_EACHROUND = 200
class Scheduler:
	def __init__(self):
		#Import weibo accounts from json
		#[{'user':'xxx', 'psw':'xxx'}, ...]'
		acFile = file("accounts.json")
		self.accounts = json.load(acFile)
		acFile.close()
		#Import other configs(DB)
		confFile = file("config.json")
		self.config = json.load(confFile)
		confFile.close()

		logging.config.dictConfig(self.config['log'])
		self.logger = logging.getLogger()

		dbconfig = self.config['db']
		self.dbConn = MySQLdb.connect(host=dbconfig['host'], user=dbconfig['user'], passwd=dbconfig['passwd'], db=dbconfig['name'], charset='utf8')
		self.dbCur = dbConn.cursor()
		#Init variables
		self.todoQueue = Queue()
		self.doneQueue = Queue()
		self.crawledSet = set()
		self.numCrawled = 0
		#Retrive info of last execution from DB
		self.logger.info('Retrieving information from database...')
		self.retrieveFromDB()
		#Init workers
		self.logger.info('Connecting with remote servers...')
		connList = []
		threadList = []
		crawlerList = []
		serverPool = list(rpyc.discover("CRAWLER"))
		for serverID, server in enumerate(serverPool):
			conn = rpyc.connect(server[0], server[1])
			bgThread = rpyc.BgServingThread(conn)
			accountList = accounts[serverID*len(accounts)/len(serverPool):(serverID+1)*len(accounts)/len(serverPool)]
			crawler = conn.root.crawler(serverID, accountList, self.fetchNewJob)
			connList.append(conn)
			threadList.append(bgThread)
			crawlerList.append(crawler)

	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		for thread in threadList:
			thread.stop()
		for conn in connList:
			conn.close()
		self.dbCur.close()
		self.dbConn.close()
		self.logger.info("%d user data are crawled in this execution" % self.numCrawled)
		return

	def retrieveFromDB(self):
		#Get users already crawled
		SQL = 'SELECT `uid` FROM `crawledUID` WHERE 1'
		self.dbCur.execute(SQL)
		result = self.dbCur.fetchall()
		map(lambda x: self.crawledSet.add(x[0]), result)
		#Get users that are friends of crawled users but not crawled
		SQL = 'SELECT T1.`followedID` FROM `crawledUID` as U, `following` as T1, `following` as T2 WHERE '
		SQL += 'T1.`followingID`=T2.`followedID` and T1.`followedID`=T2.`followingID` and T1.`followingID`=U.`uid`'
		self.dbCur.execute(SQL)
		result = self.dbCur.fetchall()
		toCrawlDict = {}
		for uid in result:
			if uid[0] not in self.crawledSet:
				toCrawlDict[uid[0]] = toCrawlDict[uid[0]]+1 if toCrawlDict.get(uid[0]) else 1
		sortedList = sorted(toCrawlDict.iteritems(), key=itemgetter(1), reverse=True)
		#Only choose those most valuable user if more than 200 uid to add
		for i in xrange(min(MAXNUM_TOCRAWL_EACHROUND, len(sortedList))):
			self.todoQueue.put(sortedList[i][0])


	def getPriorUIDs(self):
		SQL＝ "SELECT `uid` FROM `priorUID` WHERE 1"
		self.dbCur.execute(SQL)
		result = self.dbCur.fetchall()
		uidList = map(lambda x: x[0], result)
		SQL = "TRUNCATE `priorUID`"
		self.dbCur.execute(SQL)
		return uidList

	def fetchNewJob(self, serverID, prevJob, frList):
		if prevJob != None:
			todoQueue.task_done()
			doneQueue.put({'sid':serverID, 'uid':prevJob, 'frList':frList.copy()})
		try:
			newJob = todoQueue.get(True, 5)
		except Empty:
			newJob = None
		return newJob

	def work(self):
		canceled = False
		#setupWorkersNodes & let them block in todoQueue.get()
		while not canceled:
			#Check if anymore uid can be add to todoQueue
			priorUIDList = self.getPriorUIDs()
			map(lambda x: self.todoQueue(x), priorUIDList)
			toCrawlDict = {}
			while not self.doneQueue.empty():
				doneJobInfo = self.doneQueue.get_nowait()
				self.crawledSet.add(doneJob['uid'])
				#map(lambda x: toCrawlSet.add(x), doneJobInfo['frList'])
				for fr in doneJobInfo['frList']:
					if fr not in self.crawledSet:
						toCrawlDict[fr] = toCrawlDict[fr]+1 if toCrawlDict.get(fr) else 1
			sortedList = sorted(toCrawlDict.iteritems(), key=itemgetter(1), reverse=True)
			#Only choose those more valuable user if more than 1k uid to add
			for i in xrange(min(MAXNUM_TOCRAWL_EACHROUND, len(sortedList))-len(priorUIDList)):
				self.todoQueue.put(sortedList[i][0])
			self.logger.info("Uids to crawl: %r" % sortedList[:min(MAXNUM_TOCRAWL_EACHROUND, len(sortedList))])
			#Wait until this round of jobs done
			sTime = time()
			try:
				self.todoQueue.join()
			except KeyboardInterrupt:
				canceled = True
			eTime = time()
			self.numCrawled += self.doneQueue.qsize()
			self.logger.info("%d users crawled in %fs, %f/s" % (
				min(MAXNUM_TOCRAWL_EACHROUND, len(sortedList)), eTime-sTime,
				min(MAXNUM_TOCRAWL_EACHROUND, len(sortedList))/(eTime-sTime)))
			self.logger.info("%d users are done in total" % self.numCrawled)
		self.summary()

	def summary(self):
		pass

#if __name__ == "__main__":
	#with Scheduler() as ins:
		#ins.work()