import time
import sched
import os
import sys
import json
import logging
import argparse
from redis import Redis
from datetime import datetime, timedelta
from ConfigParser import ConfigParser

import rq
from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.connections import use_connection
from croniter import croniter

import redis


logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logFormatter)
logger.addHandler(logger_handler)


class secrunner():
    def __init__(self, interval):
        self.time = datetime.now()
        self.interval = interval

    def get_next(self, ret_type=datetime):
        self.time = self.time + timedelta(seconds=self.interval)
        if ret_type == datetime:
            return self.time
        elif ret_type == float:
            return time.mktime(self.time.timetuple())


class HJScheduler(sched.scheduler):
    def __init__(self, status_dir, *args, **kwargs):
        self.status_dir = status_dir
        sched.scheduler.__init__(self, *args, **kwargs)

    def enterabs(self, time, priority, action, argument, prev_job_id=None):
        def action_wrapper():
            logger.debug("Running %s" % action.__name__)
            open('{status_path}/{action_name}.status'.format(
                status_path=self.status_dir,
                action_name=action.__name__), 'w')
            action(prev_job_id)

        logger.debug("Scheduling at %s %s" % (datetime.fromtimestamp(time), action.__name__))
        return sched.scheduler.enterabs(self, time, priority, action_wrapper, argument)

    def get_timeiter(self, delay_or_cron):
        if isinstance(delay_or_cron, int):
            return secrunner(delay_or_cron)
        if isinstance(delay_or_cron, basestring):
            return croniter(delay_or_cron)

    def repeat(self, delay_or_cron, priority, action, argument=(), name=None, single_job=False):
        if name is None:
            name = action.__name__
        this = self
        timeiter = self.get_timeiter(delay_or_cron)

        def action_wrapper(prev_job_id=None):
            _should_run = True
            if single_job and prev_job_id:
                try:
                    prev_job = Job.fetch(prev_job_id)
                except NoSuchJobError:
                    pass
                else:
                    _should_run = prev_job.is_finished or prev_job.is_failed
                    if not _should_run:
                        logger.error('HJScheduler: previous job [%s] is still running' % name)
            if _should_run:
                job = action()
                current_job_id = job.id
            else:
                current_job_id = prev_job_id
            this.enterabs(timeiter.get_next(ret_type=float), priority, action_wrapper, argument, prev_job_id=current_job_id)

        action_wrapper.__name__ = str(name.rpartition('.')[2])
        self.enterabs(timeiter.get_next(ret_type=float), priority, action_wrapper, argument)

def _task(config, job):
    q = rq.Queue(job.get("queue") or config["default_queue"])
    return lambda *args: q.enqueue(job["name"], *args)


def run_scheduler():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--config', dest='config', action='store', required=True,
                       help='path to config file')

    args = parser.parse_args()
    config = json.load(open(vars(args)["config"]))
    use_connection(Redis(config["redis"]))
    try:
        os.mkdir(config["status_dir"])
    except OSError:
        pass

    scheduler = HJScheduler(config["status_dir"], time.time, time.sleep)
    for job in config["jobs"]:

        job_task = lambda job: job
        scheduler.repeat(job.get("cron") or job["interval"], 1, _task(config, job), name=job['name'])

    logger.debug("running")
    scheduler.run()

if __name__ == "__main__":
    run_scheduler()
