from __future__ import absolute_import
import os
import json
import time
import logging
import argparse
from redis import Redis

import rq
from rq_cron import RQCron
from rq.connections import use_connection


logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] %(message)s")
logger = logging.getLogger('rq-cron')
logger.setLevel(logging.DEBUG)
logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logFormatter)
logger.addHandler(logger_handler)


def _task(config, job):
    q = rq.Queue(job.get("queue") or config["default_queue"])
    return lambda *args: q.enqueue(job["name"], *args)


def run_scheduler():
    parser = argparse.ArgumentParser(description='RQ cron')
    parser.add_argument('--config', dest='config', action='store',
                        required=True, help='path to config file')

    args = parser.parse_args()
    config = json.load(open(vars(args)["config"]))
    use_connection(Redis(config["redis"]))
    try:
        os.mkdir(config["status_dir"])
    except OSError:
        pass

    scheduler = RQCron(config["status_dir"], time.time, time.sleep)
    for job in config["jobs"]:
        scheduler.repeat(job.get("cron") or job["interval"], 1, _task(config, job), 
                         name=job['name'])

    logger.debug("running")
    scheduler.run()
