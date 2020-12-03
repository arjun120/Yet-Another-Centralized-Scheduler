# Yet-Another-Centralized-Scheduler
YACS is a centralized scheduler which schedules tasks among various workers in a distributed environment.

## Design
A configuration file can be passed to the Master node indicating the number of worker nodes and the number of slots in each node.

## Usage

Run the following bash commands in the same order:

For the Master node - 
```bash
python3 Master.py config.json <scheduler_type>
```
The different scheduler types implemented are LL, RR and R.

For each of the worker nodes - 
```bash
python3 Worker.py <port_number> <worker_id>
```

Finally we run the requests.py to generate and send the job requests to the Master:
```bash
python3 requests.py <number_of_requests>
```

 Following the above mentioned steps generates two log files, one for the master and the other for the workers.
 The analysis is done on these log files, the analyzer can be run using the following command - 
 ```bash
 python3 yacs_analyzer.py master.log worker.log
 ```
Running the analyzer generates two graphs, each of these supporting the execution statistics displayed on the console.

## License
[MIT](https://choosealicense.com/licenses/mit/)
 
