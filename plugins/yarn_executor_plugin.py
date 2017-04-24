__author__ = 'robertsanders'

from airflow.plugins_manager import AirflowPlugin
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import State
from airflow.utils.logging import LoggingMixin

from queue import Queue

import time
import multiprocessing
import logging
import knit
import os

"""
export HADOOP_CONF_DIR=/etc/hadoop/conf

pip install knit

pip install conda

airflow.cfg:
executor = YarnExecutor
"""

#https://media.readthedocs.org/pdf/knit/latest/knit.pdf
"""
NOTE: This requires taht airflow be installed in conda to create a virtual env in the yarn containers
"""


# k = knit.Knit()
# cmd = "python -c 'import sys; print(sys.path); import socket; print(socket.gethostname())'"
# appId = k.start(cmd)

autodetect = True

# NAMENODE_HOST = "quickstart.cloudera" #configuration.get('yarn', 'NAMENODE_HOST')

# CONTAINERS
# VCORES
# MEMORY

site_packages_dir = "/usr/local/lib/python2.7/site-packages/airflow"
base_files = []
for folder, subs, files in os.walk(site_packages_dir):
    for filename in files:
        base_files.append(os.path.join(folder, filename))



class YarnAppParentWorker(multiprocessing.Process, LoggingMixin):

    # env_name_prefix = "airflow"

    def __init__(self, id, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.id = id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.daemon = True
        self.knit_instance = knit.Knit(autodetect=autodetect)
        # self.env_zip = self.knit_instance.create_env(
        #     env_name=self.env_name_prefix + id,
        #     packages=['airflow'],
        #     remove=True
        # )

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if key is None:
                self.logger.warning("Received poison pill. No more tasks to run.")
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            self.logger.info("{} running {}".format(self.__class__.__name__, command))
            # command = "exec bash -c '{0}'".format(command)
            command = "$PYTHON_BIN $CONDA_PREFIX/bin/" + command
            try:
                files = [
                    "/usr/local/bin/airflow",
                    "/home/cloudera/airflow/airflow.cfg",
                    "/home/cloudera/airflow/dags/test_scheduler_long.py"
                ]
                files.extend(base_files)
                logging.info("files: " + str(files))
                # appId = self.knit_instance.start(command, env=self.env_name)
                appId = self.knit_instance.start(command, files=files)
                self.logger.info("{}: Started App with Id {}".format(self.__class__.__name__, appId))
                # self.logger.
                success = self.knit_instance.wait_for_completion()
                self.logger.info("{}: Finished execution of app '{}' with success status of {} and object: ".format(self.__class__.__name__, appId, success, self.knit_instance.status()))
                logging.info(self.knit_instance.logs(shell=True))
                state = State.SUCCESS
            except Exception as e:
                state = State.FAILED
                self.logger.error("{}: failed to execute task {}:".format(self.__class__.__name__, str(e)))
            self.result_queue.put((key, state))
            self.task_queue.task_done()
            time.sleep(1)


class YarnExecutor(BaseExecutor):

    """
    Available Variables as apart of BaseExecutor:
    self.parallelism = parallelism  #pulled from configurations
    self.queued_tasks = {}
    self.running = {}
    self.event_buffer = {}
    """
    def start(self):
        self.queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.Queue()
        self.workers = [
            YarnAppParentWorker(i, self.queue, self.result_queue) for i in range(self.parallelism)
        ]
        for w in self.workers:
            w.start()

    def execute_async(self, key, command, queue=None):
        self.queue.put((key, command))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        # Sending poison pill to all worker
        [self.queue.put((None, None)) for w in self.workers]
        # Wait for commands to finish
        self.queue.join()
        self.sync()


class YarnExecutorPlugin(AirflowPlugin):
    name = "yarn_executor_plugin"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = [YarnExecutor]
    admin_views = []
    menu_links = []
