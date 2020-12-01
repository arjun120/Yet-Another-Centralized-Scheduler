import socket
import threading
import json
import time
import sys
import logging
import math

class Worker():
    exec_pool_lock = None
    #  Worker Id
    Id = None
    #  Port on which worker listens for jobs
    jobSocket = None
    #  Logger instance
    logger = None
    exec_pool = None
    #  Constructor
    def __init__(self,jobPort,Id):
        self.Id = Id
        self.exec_pool = dict()
        self.exec_pool_lock = threading.Lock()

        self.jobSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.jobSocket.bind(('',jobPort))
        self.jobSocket.listen(1)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        fileHandler = logging.FileHandler("worker.log")
        fileHandler.setFormatter(logging.Formatter('%(threadName)s:%(asctime)s:%(levelname)s:%(message)s'))
        self.logger.addHandler(fileHandler)

    def runTasks(self):
        #  self.exec_pool = [{'task_id': '0_M0', 'duration': 4}, {'task_id': '0_M1', 'duration': 2}, {'task_id': '0_M2', 'duration': 1},{'task_id': '0_R0', 'duration': 1}]
        #  self.exec_pool = {'0_M0':{'duration': 4,'type':'M'}, '0_M1':{'duration': 2,'type':'M'},'0_M2':{'duration': 1,'type':'M'},'0_R0':{'duration': 1,'type':'R'}}
        to_be_removed = []
        time_elapsed = 0
        while 1:
            self.exec_pool_lock.acquire()
            if(list(self.exec_pool.keys()) == []):
                self.exec_pool_lock.release()
                print("No tasks in pool")
                time.sleep(1)
                continue
            for taskId in self.exec_pool.keys():
                if(self.exec_pool[taskId]['duration'] > 0):
                    self.exec_pool[taskId]['duration'] = self.exec_pool[taskId]['duration'] - 1
            time_elapsed = time_elapsed + 1
            time.sleep(1)
            for taskId in self.exec_pool.keys():
                if(self.exec_pool[taskId]['duration'] == 0):
                    to_be_removed.append(taskId)
                    taskInfo = {k:self.exec_pool[taskId][k] for k in self.exec_pool[taskId].keys()}
                    taskInfo['task_id'] = taskId
                    taskInfo['duration'] = math.floor(time.time() - taskInfo['arrived'])
                    self.logger.info("Task "+taskInfo['task_id'] + " ran for "+str(taskInfo['duration']) +" on worker "+str(self.Id))
                    print("Task "+taskInfo['task_id'] + " ran for "+str(taskInfo['duration']) +" on worker "+str(self.Id))
                    taskInfo['worker_id'] = self.Id
                    masterSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    masterSocket.connect(('',5001))
                    masterSocket.send(json.dumps(taskInfo).encode())
                    self.logger.debug("Task " + taskInfo['task_id'] + " completed on worker " + str(self.Id)+".Status sent to master.")
                    masterSocket.close()
            for i in to_be_removed:
                self.exec_pool.pop(i)
            to_be_removed.clear()
            self.exec_pool_lock.release()

    #  Handle incoming task info for running tasks.Continuously listens for requests on port passed.
    def handleTaskRequest(self):
        while 1:
            connectionSocket, addr = self.jobSocket.accept()
            #  taskInfo = [job_id,type of job,taskId,duration]
            taskInfo = json.loads(connectionSocket.recv(1024).decode())
            arrival_time = time.time()
            connectionSocket.close()
            self.logger.info("Task " + taskInfo['task_id'] + " arrived on worker " + str(self.Id))
            self.exec_pool_lock.acquire()
            self.exec_pool[taskInfo['task_id']] = {'duration':taskInfo['duration'],'type':taskInfo['type'],'jobId':taskInfo['jobId'],'arrived':arrival_time}
            self.exec_pool_lock.release()

    #  Add worker Id to the receieved data and forward to master
    def taskEnd(self,taskInfo):
        taskInfo['worker_id'] = self.Id
        masterSocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        masterSocket.connect(('',5001))
        masterSocket.send(json.dumps(taskInfo).encode())
        self.logger.debug("Task " + taskInfo['task_id'] + " completed on worker " + str(self.Id)+".Status sent to master.")
        masterSocket.close()

    def main(self):
        t1 = threading.Thread(target = self.handleTaskRequest)
        t2 = threading.Thread(target = self.runTasks)
        t1.start()
        t2.start()

port = int(sys.argv[1])
Id = int(sys.argv[2])
w = Worker(port,Id)
w.main()
