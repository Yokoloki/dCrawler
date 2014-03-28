# -*- coding: utf-8 -*-
import sys
import rpyc
import json
import MySQLdb
import random
import logging, logging.config, logging.handlers
from operator import itemgetter
from Queue import Queue, Empty
from time import time, localtime, strftime, sleep
from copy import deepcopy

reload(sys)
sys.setdefaultencoding("utf-8")

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
		acFile = file("accounts.json")
		self.accounts = json.load(acFile)
		self.accounts = self.accounts['usable']
		random.shuffle(self.accounts)
		acFile.close()
		#Import other configs(DB)
		confFile = file("config.json")
		self.config = json.load(confFile)
		confFile.close()

		logging.config.dictConfig(self.config['log'])
		self.logger = logging.getLogger()

		self.MAXNUM_TOCRAWL_EACHROUND = self.config['MAXNUM_TOCRAWL_EACHROUND']
		self.JOIN_TIMEOUT_PERROUNT = self.config['JOIN_TIMEOUT_PERROUNT']

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
		stTime = strftime("%Y-%m-%d %H:%M:%S", localtime())
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
				toCrawlList = map(lambda x:x[0], sortedList[:min(self.MAXNUM_TOCRAWL_EACHROUND, len(sortedList))])
				for i in xrange(len(toCrawlList)):
					self.todoQueue.put(toCrawlList[i])
				self.logger.info("Uids to crawl: %r" % toCrawlList)
			else:
				self.logger.info("Uids to crawl: %r" % priorUIDList)
			#Wait until this round of jobs done
			sTime = time()
			try:
				self.todoQueue.join_with_timeout(self.JOIN_TIMEOUT_PERROUNT)
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
			self.logger.info("%d users are crawled since %s" % (self.numCrawled, stTime))
	
	def selectiveWork(self):
		#Retrive info of last execution from DB
		stTime = strftime("%Y-%m-%d %H:%M:%S", localtime())
		self.logger.info('Retrieving information from database...')
		SQL = "SELECT `uid` FROM `prioruid` WHERE 1"
		self.dbCur.execute(SQL)
		resultSet = self.dbCur.fetchall()
		delSQL = "DELETE FROM `prioruid` WHERE `uid`=%s"
		tocrawlList = [result[0] for result in resultSet]
		canceled = False
		reInit = False
		#setupWorkersNodes & let them block in todoQueue.get()
		while not canceled:
			todel = []
			while not self.doneQueue.empty():
				doneJobInfo = self.doneQueue.get_nowait()
				if doneJobInfo['uid'] in tocrawlList:
					tocrawlList.remove(doneJobInfo['uid'])
					todel.append(int(doneJobInfo['uid']))
			if len(todel) > 0:
				self.dbCur.executemany(delSQL, todel)
				self.dbConn.commit()
			if reInit:
				self.logger.info('Reinit slave nodes')
				self.initSlaveNodes()
			for i in xrange(self.MAXNUM_TOCRAWL_EACHROUND):
				self.todoQueue.put(tocrawlList[i])
			self.logger.info("Uids to crawl: %r" % tocrawlList[:self.MAXNUM_TOCRAWL_EACHROUND])
			#Wait until this round of jobs done
			sTime = time()
			try:
				self.todoQueue.join_with_timeout(self.JOIN_TIMEOUT_PERROUNT)
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
			self.logger.info("%d users are crawled since %s" % (self.numCrawled, stTime))


	def workForUpdate(self):
		#Retrive info of last execution from DB
		stTime = strftime("%Y-%m-%d %H:%M:%S", localtime())
		self.logger.info('Retrieving information from database...')
		SQL = "SELECT `uid` FROM `crawledUID` WHERE 1 ORDER BY `lastUpdate`"
		self.dbCur.execute(SQL)
		resultSet = self.dbCur.fetchall()

		toUpdateList = [result[0] for result in resultSet]
		canceled = False
		reInit = False
		#setupWorkersNodes & let them block in todoQueue.get()
		while not canceled:
			while not self.doneQueue.empty():
				doneJobInfo = self.doneQueue.get_nowait()
				if doneJobInfo['uid'] in toUpdateList:
					toUpdateList.remove(doneJobInfo['uid'])
				self.logger.debug("%d crawled" % doneJobInfo['uid'])

			if reInit:
				self.logger.info('Reinit slave nodes')
				self.initSlaveNodes()

			for i in xrange(self.MAXNUM_TOCRAWL_EACHROUND):
				self.todoQueue.put(toUpdateList[i])

			self.logger.info("Uids to update: %r" % toUpdateList[:self.MAXNUM_TOCRAWL_EACHROUND])
			#Wait until this round of jobs done
			sTime = time()
			try:
				self.todoQueue.join_with_timeout(self.JOIN_TIMEOUT_PERROUNT)
			except KeyboardInterrupt:
				canceled = True
			except JoinTimeout:
				reInit = True
				self.logger.error('todoQueue join timeout')
			eTime = time()
			self.numCrawled += self.doneQueue.qsize()
			self.logger.info("%d users updated in %fs, %f/s" % (
				self.doneQueue.qsize(), eTime-sTime,
				(self.doneQueue.qsize()/(eTime-sTime))))
			self.logger.info("%d users are updated since %s" % (self.numCrawled, stTime))

	def workForCompleteRelationship(self):
		#Retrive info of last execution from DB
		stTime = strftime("%Y-%m-%d %H:%M:%S", localtime())
		self.logger.info('Retrieving information from database...')
		SQL = "SELECT `uid` FROM `crawledUID` WHERE `lastUpdate`!='0000-00-00 00:00:00'"
		self.dbCur.execute(SQL)
		resultSet = self.dbCur.fetchall()
		map(lambda x: self.crawledSet.add(x[0]), resultSet)

		SQL = "SELECT `uid`, `crawlFr`, `crawlComm` FROM `priorUID` WHERE `crawlFr`!=0 OR `crawlComm`!=0"
		self.dbCur.execute(SQL)
		resultSet = self.dbCur.fetchall()
		toexpandList = [(result[0], result[1], result[2]) for result in resultSet]
		currJob = None
		relatedUID = []
		reInit = False
		#setupWorkersNodes & let them block in todoQueue.get()
		while toexpandList or currJob:
			tdbConn = MySQLdb.connect(host=self.dbConfig['host'], user=self.dbConfig['user'], passwd=self.dbConfig['passwd'], db=self.dbConfig['name'], charset='utf8')
			tdbCur = self.dbConn.cursor()
			while not self.doneQueue.empty():
				doneJobInfo = self.doneQueue.get_nowait()
				if doneJobInfo['uid'] in relatedUID:
					relatedUID.remove(doneJobInfo['uid'])
				self.logger.debug("%d crawled" % doneJobInfo['uid'])

			if reInit:
				self.logger.info('Reinit slave nodes')
				self.initSlaveNodes()
			if not relatedUID:
				if currJob and currJob[0] in self.crawledSet:
					SQL = "DELETE FROM `priorUID` WHERE `uid`=%d" % currJob[0]
					tdbCur.execute(SQL)
					SQL = "UPDATE `crawledUID` SET `frCrawled`=%d, `commCrawled`=%d WHERE `uid`=%d" % (currJob[1], currJob[2], currJob[0])
					tdbCur.execute(SQL)
					tdbConn.commit()
					currJob = toexpandList.pop()
				elif currJob and currJob[0] not in self.crawledSet:
					self.todoQueue.put(currJob[0])
					self.logger.info('Crawl %d first', currJob[0])
					try:
						self.todoQueue.join_with_timeout(self.JOIN_TIMEOUT_PERROUNT)
					except JoinTimeout:
						reInit = True
						self.logger.error('todoQueue join timeout')
						continue
					self.crawledSet.add(currJob[0])
				elif not toexpandList:
					self.logger.info('All expand jobs finished')
					break
				else:
					currJob = toexpandList.pop()
				self.logger.info('Start to expand %d' % currJob[0])
				if currJob[1]:
					SQL = 'SELECT T1.`followedID` FROM `following` as T1, `following` as T2 WHERE '
					SQL += 'T1.`followerID`=T2.`followedID` and T1.`followedID`=T2.`followerID` and T1.`followerID`=%d' % currJob[0]
					tdbCur.execute(SQL)
					resultSet = tdbCur.fetchall()
					relatedUID += map(lambda x: x[0], resultSet)
					self.logger.info('Fr count of %d: %d' % (currJob[0], len(resultSet)))
				if currJob[2]:
					#@
					SQL = "SELECT DISTINCT a.`uid` FROM (SELECT `mid` FROM `posting` WHERE `uid`=%d) AS p, `weiboAT` AS a WHERE p.`mid`=a.`mid`" % currJob[0]
					tdbCur.execute(SQL)
					resultSet = tdbCur.fetchall()
					for result in resultSet:
						if result[0] not in relatedUID:
							relatedUID.append(result[0])
					self.logger.info('@uid of %d: %d' % (currJob[0], len(resultSet)))
					#BE @
					SQL = "SELECT DISTINCT p.`uid` FROM (SELECT `mid` FROM `weiboAT` WHERE `uid`=%d) AS a, `posting` AS p WHERE a.`mid`=p.`mid`" % currJob[0]
					tdbCur.execute(SQL)
					resultSet = tdbCur.fetchall()
					for result in resultSet:
						if result[0] not in relatedUID:
							relatedUID.append(result[0])
					self.logger.info('Be@uid of %d: %d' % (currJob[0], len(resultSet)))
					#REPOST
					SQL = "SELECT DISTINCT p2.`uid` FROM (SELECT `originMid` FROM (SELECT `mid` FROM `posting` WHERE `uid`=%d) AS p1, `weiboInfo` AS info WHERE p1.`mid`=info.`mid` AND info.`isRepost`=1) AS rep, `posting` AS p2 WHERE rep.`originMid`=p2.`mid`" % currJob[0]
					tdbCur.execute(SQL)
					resultSet = tdbCur.fetchall()
					for result in resultSet:
						if result[0] not in relatedUID:
							relatedUID.append(result[0])
					self.logger.info('Repost uid of %d: %d' % (currJob[0], len(resultSet)))
					#COMMENT
					SQL = "SELECT DISTINCT ci.`uid` FROM `commenting` AS ci, (SELECT `cid` FROM `commentof`, `posting` WHERE `posting`.`mid`=`commentof`.`mid` AND `posting`.`uid`=%d) AS co WHERE ci.`cid`=co.`cid`" % currJob[0]
					tdbCur.execute(SQL)
					resultSet = tdbCur.fetchall()
					for result in resultSet:
						if result[0] not in relatedUID:
							relatedUID.append(result[0])
					self.logger.info('Comment uid of %d: %d' % (currJob[0], len(resultSet)))
			
			tdbCur.close()
			tdbConn.close()
			i = 0
			self.logger.info('Expanding %d: %d to go' % (currJob[0], len(relatedUID)))
			while i < self.MAXNUM_TOCRAWL_EACHROUND and i < len(relatedUID):
				if relatedUID[i] in self.crawledSet:
					relatedUID.remove(relatedUID[i])
				else:
					self.todoQueue.put(relatedUID[i])
					i += 1
			self.logger.info("Uids to update: %r" % relatedUID[:self.MAXNUM_TOCRAWL_EACHROUND])
			#Wait until this round of jobs done
			sTime = time()
			try:
				self.todoQueue.join_with_timeout(self.JOIN_TIMEOUT_PERROUNT)
			except KeyboardInterrupt:
				canceled = True
			except JoinTimeout:
				reInit = True
				self.logger.error('todoQueue join timeout')
			eTime = time()
			self.numCrawled += self.doneQueue.qsize()
			self.logger.info("%d users updated in %fs, %f/s" % (
				self.doneQueue.qsize(), eTime-sTime,
				(self.doneQueue.qsize()/(eTime-sTime))))
			self.logger.info("%d users are updated since %s" % (self.numCrawled, stTime))
	


if __name__ == "__main__":
	with Scheduler() as ins:
		#ins.work()
		#ins.workForCompleteRelationship()
		ins.selectiveWork()