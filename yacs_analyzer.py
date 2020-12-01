import sys
import re
from datetime import datetime
from statistics import median
from statistics import mean
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

master_log = open(sys.argv[1])
worker_log = open(sys.argv[2])

job_arrival_pattern = r'Thread-[\d]:(.+):INFO:Job\s([\d]+)\sarrived\.'
job_completion_pattern = r'Thread-[\d]:(.+):INFO:.*\s.*\s([\d]+)_[MR]{1}[\d]+\scompleted\son\sworker\s[\d]+'
scheduler_type_pattern = r'.*:(.+)'

job_execution_stats = dict()
match_flag = 0
scheduler_type = ''
for line in master_log:
    line = line.strip()
    if not match_flag:
        scheduler_type_m = re.match(scheduler_type_pattern, line)
        if scheduler_type_m:
            scheduler_type = scheduler_type_m.group(1)
        match_flag = 1
    job_arr = re.match(job_arrival_pattern, line)
    job_com = re.match(job_completion_pattern,line)    
    if job_arr:
        job_execution_stats[job_arr.group(2)] = [job_arr.group(1)]
        
    elif job_com:
        if(len(job_execution_stats[job_com.group(2)]) == 2):
            job_execution_stats[job_com.group(2)][1] = job_com.group(1)
        else:
            job_execution_stats[job_com.group(2)].append(job_com.group(1))

for job_id in job_execution_stats:
    job_execution_stats[job_id].append((datetime.strptime(job_execution_stats[job_id][1],'%Y-%m-%d %H:%M:%S,%f') - datetime.strptime(job_execution_stats[job_id][0],'%Y-%m-%d %H:%M:%S,%f')).total_seconds())

task_arrival_pattern = r'Thread-[\d]:(.+):INFO:Task\s(.+)\sarrived\son\sworker\s([\d]+)'
task_completion_pattern = r'Thread-[\d]:(.+):INFO:Task\s(.+)\sran\sfor\s\d+\son\sworker\s([\d]+)'

task_execution_stats = dict()
task_count_workers = dict()
graph_info = dict()

for line in worker_log:
    line = line.strip()
    task_arr = re.match(task_arrival_pattern, line)
    task_com = re.match(task_completion_pattern, line)
    if task_arr:
        if task_arr.group(3) not in task_execution_stats.keys():
            task_execution_stats[task_arr.group(3)] = dict()
            task_count_workers[task_arr.group(3)] = 0
            graph_info[task_arr.group(3)] = [[],[]]
        task_execution_stats[task_arr.group(3)][task_arr.group(2)] = [task_arr.group(1)]
        task_count_workers[task_arr.group(3)] += 1
        if task_arr.group(1) in graph_info[task_arr.group(3)][0]:
            graph_info[task_arr.group(3)][1][graph_info[task_arr.group(3)][0].index(task_arr.group(1))] = task_count_workers[task_arr.group(3)]
        else:
            graph_info[task_arr.group(3)][0].append(datetime.strptime(task_arr.group(1),'%Y-%m-%d %H:%M:%S,%f'))
            graph_info[task_arr.group(3)][1].append(task_count_workers[task_arr.group(3)])
    if task_com:
        task_execution_stats[task_com.group(3)][task_com.group(2)].append(task_com.group(1))
        task_count_workers[task_com.group(3)] -= 1
        if task_com.group(1) in graph_info[task_com.group(3)][0]:
            graph_info[task_com.group(3)][1][graph_info[task_com.group(3)][0].index(task_com.group(1))] = task_count_workers[task_com.group(3)]
        else:
            graph_info[task_com.group(3)][0].append(datetime.strptime(task_com.group(1),'%Y-%m-%d %H:%M:%S,%f'))
            graph_info[task_com.group(3)][1].append(task_count_workers[task_com.group(3)])

for worker in task_execution_stats.keys():
    for task_id in task_execution_stats[worker].keys():
        task_execution_stats[worker][task_id].append((datetime.strptime(task_execution_stats[worker][task_id][1],'%Y-%m-%d %H:%M:%S,%f') - datetime.strptime(task_execution_stats[worker][task_id][0],'%Y-%m-%d %H:%M:%S,%f')).total_seconds())


job_completion_times = []
task_completion_times = []
print('Yet Another Centralized Scheduler: ',"\033[1m" + scheduler_type + "\033[0m")
print('-'*25)
print('Job Exectution Statistics')
print('-'*28)
print("\033[1m" + 'JobId\tTotal Execution time' + "\033[0m")
print('-'*28)
for job_id in job_execution_stats:
    print(job_id, job_execution_stats[job_id][2],sep='\t')
    job_completion_times.append(job_execution_stats[job_id][2])
print('-'*33)
print('Mean Job execution time: ',mean(job_completion_times))
print('Median Job execution time: ',median(job_completion_times))
print('-'*33)
print('Task Exectution Statistics')
print('-'*44)
print("\033[1m" + 'WorkerId\tJobId\tTotal Execution time' + "\033[0m")
current_worker = '-1'
for worker in task_execution_stats.keys():
    for task_id in task_execution_stats[worker].keys():
        if worker != current_worker:
            print('-'*44)
            print(worker, task_id, task_execution_stats[worker][task_id][2],sep='\t\t')
        else:
            print(' ', task_id, task_execution_stats[worker][task_id][2],sep='\t\t')
        task_completion_times.append(task_execution_stats[worker][task_id][2])
        current_worker = worker
print('-'*44)
print('Mean Task execution time: ',mean(task_completion_times))
print('Median Task execution time: ',median(task_completion_times))
print('-'*44)

colors = iter(cm.plasma(np.linspace(0, 1, 5)))

labels = ['Job Execution time','Task Execution time']
means = [mean(job_completion_times), round(mean(task_completion_times),4)]
medians = [median(job_completion_times), round(median(task_completion_times),4)]

x = np.arange(len(labels)) 
width = 0.35 

fig, ax = plt.subplots()
bars1 = ax.bar(x - width/2, means, width, label='Mean',color=next(colors))
bars2 = ax.bar(x + width/2, medians, width, label='Median',color=next(colors))

ax.set_ylabel('Time(in seconds)')
ax.set_title('Execution time for jobs/tasks')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
def name(bars):
    for bar in bars:
        height = bar.get_height()
        ax.annotate('{}'.format(height),xy=(bar.get_x() + bar.get_width() / 2, height),xytext=(0, 3), textcoords="offset points",ha='center', va='bottom')
name(bars1)
name(bars2)
fig.tight_layout()
plt.savefig('Statistics for' + scheduler_type.strip('Using') + '.png')

from matplotlib.pyplot import figure

figure(figsize = (15,9))
fig, axs = plt.subplots(3)
for ax in axs.flat:
    ax.set(xlabel='Time')
axs[1].set(ylabel='Number of tasks scheduled on workers')

index = 0
for worker in graph_info.keys():
    axs[index].plot(graph_info[worker][0],graph_info[worker][1],label='Worker' + worker,color=next(colors))
    axs[index].legend()
    index += 1
plt.savefig('Task_distribution_for_' + scheduler_type.strip('Using') + '.png')    

