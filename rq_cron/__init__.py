import time
import sched
import logging
from datetime import datetime, timedelta

from croniter import croniter
from rq.exceptions import NoSuchJobError
from rq.job import Job

logger = logging.getLogger('rq-cron')

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


class RQCron(sched.scheduler):
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

        logger.debug("Scheduling at %s %s" %
                     (datetime.fromtimestamp(time), action.__name__))
        return sched.scheduler.enterabs(self, time, priority,
                                        action_wrapper, argument)

    def get_timeiter(self, delay_or_cron):
        if isinstance(delay_or_cron, int):
            return secrunner(delay_or_cron)
        if isinstance(delay_or_cron, basestring):
            return croniter(delay_or_cron)

    def repeat(self, delay_or_cron, priority, action, argument=(),
               name=None, single_job=False):
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
                        logger.error('previous job [%s] still running' % name)
            if _should_run:
                job = action()
                current_job_id = job.id
            else:
                current_job_id = prev_job_id
            this.enterabs(timeiter.get_next(ret_type=float), priority,
                          action_wrapper, argument, prev_job_id=current_job_id)

        action_wrapper.__name__ = str(name.rpartition('.')[2])
        self.enterabs(timeiter.get_next(ret_type=float), priority,
                      action_wrapper, argument)
