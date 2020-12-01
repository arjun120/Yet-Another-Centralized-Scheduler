import socket
import threading
import json
import time
import sys
import random
import logging

class Master():
    #  Simple mutex lock
    jobQueuelock = threading.Lock()
    workerStatuslock = threading.Lock()
    #  Logger instance
    logger = None
    #  Info about workers from config.json.Shared between threads.
    workerData = dict()
    #  Socket on which master listens for job requests
    jobsSocket = None
    #  Socket on which master listens for task completion messages from workers
    workerSocket = None
    #  Status of all running jobs.Shared between threads.
    jobStatus = dict()
    #  Type of scheduling to be used
    scheduling = None
    #  Constructor
    def __init__(self,jobsPort,workerPort):
        configFile = open(sys.argv[1])
        workerConfig = json.load(configFile)['workers']
        configFile.close()

        #  Accept type of scheduling as a command line argument
        self.scheduling = sys.argv[2]

        #  Initialise logger instance
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        fileHandler = logging.FileHandler("master.log")
        fileHandler.setFormatter(logging.Formatter('%(threadName)s:%(asctime)s:%(levelname)s:%(message)s'))
        self.logger.addHandler(fileHandler)

        #  Bind sockets to jobsPort  and workerPort
        self.jobsSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.workerSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.jobsSocket.bind(('localhost',jobsPort))
        self.workerSocket.bind(('localhost',workerPort))
        self.jobsSocket.listen(500)
        self.workerSocket.listen(500)

        #  Initialise worker data in a dictionary
        for worker in workerConfig:
            self.workerData[worker['worker_id']] = [worker['slots'],worker['port']]

    #  Responsible for handling job requests coming on the jobsPort.
    def handleJobRequest(self):
        jobData = None
        while 1:
            #  self.logger.debug("In handleJobRequest")
            #  print("In handleJobRequest")
            #  Receieve job request data
            connectionSocket, addr = self.jobsSocket.accept()
            jobData = json.loads(connectionSocket.recv(4096).decode())
            jobId = jobData['job_id']
            print("Job request with ID " + str(jobId) + " arrived.")
            connectionSocket.close()

            self.logger.info("Job " + str(jobId) + " arrived.")
            #  Acquire lock to prevent race conditions.
            self.jobQueuelock.acquire()
            #  print("Lock acquired")

            #  Populate jobStatus with the job request data based on type of task.
            self.jobStatus[jobId] = dict()
            self.jobStatus[jobId]['M'] = dict()
            self.jobStatus[jobId]['R'] = dict()
            #  The value at last index is the current state of the task.
            #  None -> unscheduled
            #  0 -> scheduled and running
            #  1 -> scheduled and complete
            for task in jobData['map_tasks']:
                self.jobStatus[jobId]['M'][task['task_id']] = [task['duration'],None]
            for task in jobData['reduce_tasks']:
                self.jobStatus[jobId]['R'][task['task_id']] = [task['duration'],None]

            self.jobQueuelock.release()
            #  print("Lock released")

    #  Handle worker messages about task completion on workerPort.
    def handleTaskComplete(self):
        while 1:
            connectionSocket, addr = self.workerSocket.accept()
            #  taskinfo = [jobId,'M' or 'R',task_id,duration,worker_id]
            taskInfo = json.loads(connectionSocket.recv(4096).decode())
            self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " completed on worker " + str(taskInfo['worker_id']))
            connectionSocket.close()
            print("Task " + taskInfo['task_id'] + " completed on worker " + str(taskInfo['worker_id']))

            #  Acquire lock to prevent race conditions
            self.jobQueuelock.acquire()
            #  print("Lock acquired")
            #  Mark task status as 1
            self.jobStatus[taskInfo['jobId']][taskInfo['type']][taskInfo['task_id']][1] = 1
            #  Increment no of slots on the particular worker
            self.workerData[taskInfo['worker_id']][0] = self.workerData[taskInfo['worker_id']][0] + 1

            #  Remove any completed jobs from the job Pool
            self.removeCompletedJobs()

            self.jobQueuelock.release()
            #  print("Lock released")

    #  Removes completed jobs from the job pool
    def removeCompletedJobs(self):
        to_be_removed = []
        for jobId in self.jobStatus.keys():
            map_done = 0
            red_done = 0
            for task_id in self.jobStatus[jobId]['M']:
                if(self.jobStatus[jobId]['M'][task_id][1] == 1):
                    map_done = map_done + 1
            for task_id in self.jobStatus[jobId]['R']:
                if(self.jobStatus[jobId]['R'][task_id][1] == 1):
                    red_done = red_done + 1
            if(map_done == len(self.jobStatus[jobId]['M']) and red_done == len(self.jobStatus[jobId]['R'])):
                to_be_removed.append(jobId)
        for jobId in to_be_removed:
            self.jobStatus.pop(jobId)
            print("Removing job " + str(jobId))



    def RRScheduler(self):
        #  List of worker ID's
        workers = list(self.workerData.keys())
        no_workers = len(workers)
        #  Index of the worker in workers that will be assigned the next task.
        next_worker = 0
        while 1:
            #  Acquire lock to prevent race conditions
            self.jobQueuelock.acquire()
            #  If no job requests or all job requests handled release lock and sleep
            if(list(self.jobStatus.keys()) == []):
                print("No jobs in queue")
                self.jobQueuelock.release()
                #  print("Lock released")
                time.sleep(1)
                continue

            #  Iterate through the job pool by jobId scheduling tasks if unscheduled
            for jobId in self.jobStatus.keys():
                #  Used to determine if all map tasks are done in order to schedule reduce tasks
                map_done = 0
                #  Iterate through the map tasks present in job with jobId
                for task_id in self.jobStatus[jobId]['M'].keys():
                    #  Schedule job if unscheduled and no of slots on the worker is more than 0
                    if(self.jobStatus[jobId]['M'][task_id][1] == None):
                        if(self.workerData[workers[next_worker]][0] > 0):
                            worker = workers[next_worker]
                            workerLaunch = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                            workerJobport = self.workerData[worker][1]
                            workerLaunch.connect(('localhost',workerJobport))

                            #  taskInfo = [jobId,'M',task_id,duration]
                            taskInfo = self.dataToJSON(jobId,'M',task_id,self.jobStatus[jobId]['M'][task_id][0])
                            self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                            #  Send task request to worker
                            workerLaunch.send(json.dumps(taskInfo).encode())
                            #  critical section
                            #  Set task status to 0.Decrement worker's slots by 1.
                            self.jobStatus[jobId]['M'][task_id][1] = 0
                            self.workerData[worker][0] = self.workerData[worker][0] - 1;
                            #  Go to the next worker
                            next_worker = (next_worker + 1) % no_workers

                    #  Count no of map tasks which are complete
                    if(self.jobStatus[jobId]['M'][task_id][1] == 1):
                        map_done = map_done + 1

                #  Schedule reduce tasks only if all map tasks are complete
                if(map_done == len(self.jobStatus[jobId]['M'])):
                    #  Iterate through the reduce tasks present in job with jobId
                    for task_id in self.jobStatus[jobId]['R'].keys():
                        #  Schedule job if unscheduled and no of slots on the worker is more than 0
                        if(self.jobStatus[jobId]['R'][task_id][1] == None):
                            if(self.workerData[workers[next_worker]][0] > 0):
                                worker = workers[next_worker]
                                workerLaunch = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                workerJobport = self.workerData[worker][1]
                                workerLaunch.connect(('localhost',workerJobport))

                                #  taskInfo = [jobId,'R',task_id,duration]
                                taskInfo = self.dataToJSON(jobId,'R',task_id,self.jobStatus[jobId]['R'][task_id][0])
                                self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                                #  Send task request to worker
                                workerLaunch.send(json.dumps(taskInfo).encode())
                                #  critical section
                                #  Set task status to 0.Decrement worker's slots by 1.
                                self.jobStatus[jobId]['R'][task_id][1] = 0
                                self.workerData[worker][0] = self.workerData[worker][0] - 1;
                                next_worker = (next_worker + 1) % no_workers
                                #  Go to the next worker

            self.jobQueuelock.release()

    def RandomScheduler(self):
        while 1:
            #  Acquire lock to prevent race conditions
            self.jobQueuelock.acquire()
            #  If no job requests or all job requests handled release lock and sleep
            if(list(self.jobStatus.keys()) == []):
                self.jobQueuelock.release()
                print("No jobs in queue")
                time.sleep(1)
                continue

            #  Iterate through the job pool by jobId scheduling tasks if unscheduled
            for jobId in self.jobStatus.keys():
                #  Used to determine if all map tasks are done in order to schedule reduce tasks
                map_done = 0
                #  Schedule job if unscheduled and no of slots on the worker is more than 0
                for task_id in self.jobStatus[jobId]['M'].keys():
                    #  Schedule job if unscheduled and no of slots on the worker is more than 0
                    if(self.jobStatus[jobId]['M'][task_id][1] == None):
                        #  worker is the Worker on which next task is scheduled
                        worker = random.choice(list(self.workerData.keys()))
                        if(self.workerData[worker][0] > 0):
                            workerLaunch = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                            workerJobport = self.workerData[worker][1]
                            workerLaunch.connect(('localhost',workerJobport))

                            #  taskInfo = [jobId,'M',task_id,duration]
                            taskInfo = self.dataToJSON(jobId,'M',task_id,self.jobStatus[jobId]['M'][task_id][0])
                            self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                            #  Send task request to worker
                            workerLaunch.send(json.dumps(taskInfo).encode())
                            #  critical section
                            #  Set task status to unscheduled.Decrement worker slots by 1
                            self.jobStatus[jobId]['M'][task_id][1] = 0
                            self.workerData[worker][0] = self.workerData[worker][0] - 1;

                    #  Count no of finished map tasks for job jobId
                    if(self.jobStatus[jobId]['M'][task_id][1] == 1):
                        map_done = map_done + 1

                #  Schedule reduce tasks only if all map tasks in the job are complete
                if(map_done == len(self.jobStatus[jobId]['M'])):
                    for task_id in self.jobStatus[jobId]['R'].keys():
                        if(self.jobStatus[jobId]['R'][task_id][1] == None):
                            worker = random.choice(list(self.workerData.keys()))
                            if(self.workerData[worker][0] > 0):
                                workerLaunch = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                workerJobport = self.workerData[worker][1]
                                workerLaunch.connect(('localhost',workerJobport))
                                #  taskInfo = [jobId,'R',task_id,duration]
                                taskInfo = self.dataToJSON(jobId,'R',task_id,self.jobStatus[jobId]['R'][task_id][0])
                                self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                                #  Send task request to worker
                                workerLaunch.send(json.dumps(taskInfo).encode())
                                #  critical section
                                #  Set task status to unscheduled.Decrement worker slots by 1
                                self.jobStatus[jobId]['R'][task_id][1] = 0
                                self.workerData[worker][0] = self.workerData[worker][0] - 1;


            self.jobQueuelock.release()

    def LLScheduler(self):
        while 1:
            self.jobQueuelock.acquire()
            #  Sleep if job pool empty
            if(list(self.jobStatus.keys()) == []):
                self.jobQueuelock.release()
                print("No jobs in queue")
                #  print("Lock released")
                time.sleep(1)
                continue

            #  Iterate through the job pool by jobId scheduling tasks if unscheduled
            for jobId in self.jobStatus.keys():
                #  Maintain no of completed map tasks for the job jobId
                map_done = 0
                for task_id in self.jobStatus[jobId]['M'].keys():
                    #  Schedule unscheduled map tasks
                    if(self.jobStatus[jobId]['M'][task_id][1] == None):
                        #  self.workerStatuslock.acquire()
                        worker = self.getLeastLoaded()
                        if(worker):
                            workerLaunch = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                            workerJobport = self.workerData[worker][1]
                            workerLaunch.connect(('localhost',workerJobport))

                            #  taskInfo = [jobId,'M',task_id,duration]
                            taskInfo = self.dataToJSON(jobId,'M',task_id,self.jobStatus[jobId]['M'][task_id][0])
                            self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                            #  print("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                            #  Send task request to worker
                            workerLaunch.send(json.dumps(taskInfo).encode())
                            #  critical section
                            #  Set task status to scheduled.Decrement worker slot by 1
                            self.jobStatus[jobId]['M'][task_id][1] = 0
                            self.workerData[worker][0] = self.workerData[worker][0] - 1;
                        #  self.workerStatuslock.release()

                    #  Count no of map tasks completed
                    if(self.jobStatus[jobId]['M'][task_id][1] == 1):
                        map_done = map_done + 1

                #  Schedule reduce tasks only if all map tasks are complete
                if(map_done == len(self.jobStatus[jobId]['M'])):
                    for task_id in self.jobStatus[jobId]['R'].keys():
                        #  Schedule unscheduled reduce tasks
                        if(self.jobStatus[jobId]['R'][task_id][1] == None):
                            worker = self.getLeastLoaded()
                            if(worker):
                                workerLaunch = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                                workerJobport = self.workerData[worker][1]
                                workerLaunch.connect(('localhost',workerJobport))

                                #  taskInfo = [jobId,'R',task_id,duration]
                                taskInfo = self.dataToJSON(jobId,'R',task_id,self.jobStatus[jobId]['R'][task_id][0])
                                self.logger.info("Type:"+taskInfo['type']+" Task " + taskInfo['task_id'] + " scheduled on worker " + str(worker))
                                #  Send task request to worker
                                workerLaunch.send(json.dumps(taskInfo).encode())
                                #  critical section
                                #  Set task status to scheduled.Decrement worker slots by 1.
                                self.jobStatus[jobId]['R'][task_id][1] = 0
                                self.workerData[worker][0] = self.workerData[worker][0] - 1;

            self.jobQueuelock.release()

    #  Helper for Least Loaded Scheduler.Returns worker id of scheduler with max no of free slots
    def getLeastLoaded(self):
        next_worker = None
        max_slots = 0
        for worker in self.workerData.keys():
            if(self.workerData[worker][0] > 0 and (self.workerData[worker][0] > max_slots)):
                next_worker = worker
                max_slots = self.workerData[worker][0]
        return next_worker

    #  Converts task info into a dictionary to pass to the worker
    def dataToJSON(self,jobId,task_type,task_id,duration):
        data = {'jobId':jobId,'type':task_type,'task_id':task_id,'duration':duration}
        return data

    #  Handles creation of threads for the necessary functions
    def main(self):
        scheduler = None
        if(self.scheduling == 'RR'):
            self.logger.debug("Using Round robin scheduler")
            scheduler = self.RRScheduler
        elif self.scheduling == 'LL':
            self.logger.debug("Using Least loaded scheduler")
            scheduler = self.LLScheduler
        elif self.scheduling == 'R':
            self.logger.debug("Using Random scheduler")
            scheduler = self.RandomScheduler
        else:
            print("Invalid scheduler")
            sys.exit(1)
        t1 = threading.Thread(target = self.handleJobRequest)
        t2 = threading.Thread(target = scheduler)
        t3 = threading.Thread(target = self.handleTaskComplete)
        t1.start()
        t2.start()
        t3.start()

m = Master(5000,5001)
m.main()
