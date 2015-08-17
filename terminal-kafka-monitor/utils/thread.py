import threading
import logging


class WorkerThread(threading.Thread):
    def __init__(self, func, event, wait_interval=None, daemon=False,
            logger=None, *args, **kwargs):
        self.func = func
        self.event = event
        self.wait_interval = wait_interval
        #self.logger = logger
        #if not self.logger:
        #    self.logger = logging.getLogger(__file__)

        super(WorkerThread, self).__init__()

        #self.daemon = daemon
    def run(self):
        while not self.event.wait(self.wait_interval):
            #self.logger.debug("Running worker thread")
            self.func()
