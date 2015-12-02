#!/usr/bin/env python

from setuptools import setup

setup(name='RQ-Cron',
      version='1.0',
      description='RQ Cron',
      author='Andrii Kostenko',
      author_email='andrey@kostenko.name',
      packages=['rq_cron', 'rq_cron.scripts'],
      install_requires=['rq>=0.3.5', 'croniter'],
      entry_points='''\
         [console_scripts]
         rq-cron = rq_cron.scripts.rq_cron:run_scheduler
      ''',
      url='https://github.com/Healthjoy/rq-cron',
     )
