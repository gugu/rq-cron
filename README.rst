=======
RQ-Cron
=======

`RQ Cron <https://bitbucket.org/Healthjoy/rq-cron>`_ is a cron-like daemon, which can schedule tasks

============
Requirements
============

* `RQ <https://github.com/nvie/rq>`_
* `croniter <https://github.com/taichino/croniter>`_

============
Installation
============

    pip install RQ-Cron

=====
Usage
=====

    rq-cron --config /etc/rq_cron.json
    
========
Benefits
========

1. Simple - uses python internal sched.py
2. Stateless - don't store state
3. Allows intervals < 60 seconds
4. Saves task execution time in files for monitoring

==============
Example config
==============

Example::

	{
	   "jobs" : [
	      {
		 "queue": "queue1",
		 "name" : "module1.task1",
		 "interval" : 60
	      },
	      {
		 "interval" : 60,
		 "queue": "queue2",
		 "name" : "module2.task1"
	      },
	      {
		 "cron" : "32 */3 * * *",
		 "name" : "module3.task1"
	      },
	   ],
	   "status_dir" : "/tmp/rq_cron_status",
	   "default_queue": "queue",
	   "redis" : "localhost"
	}
