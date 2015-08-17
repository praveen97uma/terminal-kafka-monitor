FKA_CLUSTER_HOSTS = ",".join([
                            "he-kafka-alpha-vpc01.hackerearth.com:9092",
                            "he-kafka-beta-vpc02.hackerearth.com:9092",
                            "he-kafka-gamma-vpc003.hackerearth.com:9092"
                        ])


import npyscreen
import random
import logging
import math
import psutil

from utils.kafka import get_consumer_groups_offset_status
from utils.thread import WorkerThread
from widgets import MultiLineActionWidget, MonitorWindowForm


class KafkaMonitor(npyscreen.NPSApp):

    def __init__(self, stop_event, logger=None):
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger('KafkaMonitor')

        self.monitor_window = None
        self.update_worker = None
        self.stop_event = stop_event
        self.consumer_groups = None

    def while_waiting(self):
        self.logger.debug("While waiting called")
        self.logger.debug(self.update_worker)
        if not self.update_worker:
            self.logger.debug("Starting update worker")
            worker = WorkerThread(self.update_data, self.stop_event,
                    wait_interval=1)
            self.update_worker = worker
            self.update_worker.start()
            #self.update_worker.join()

    def update_data(self):
        self.logger.debug("Fetching and updating data")
        data = get_consumer_groups_offset_status(stop_event=self.stop_event)
        self.logger.debug("Retrieved data")
        self.logger.debug(data)
        self.consumer_groups.entry_widget.values = data
        self.consumer_groups.display()

    def main(self):
        #self.addForm('MAIN', KafkaManagerForm, name='KafkaManager')
        #npyscreen.setTheme(npyscreen.Themes.ElegantTheme)
        #npyscreen.setTheme(npyscreen.Themes.ColorfulTheme)

        self.keypress_timeout_default = 10
        self.monitor_window = MonitorWindowForm(parentApp=self, name="KafkaManager")


        max_y, max_x = self.monitor_window.curses_pad.getmaxyx()

        self.Y_SCALING_FACTOR = float(max_y)/27
        self.X_SCALING_FACTOR = float(max_x)/104

        self._init_consumer_groups_widget()

        self._init_helper_commands_widget()

        self.logger.debug("In window edit mode")
        self.monitor_window.edit()
        self.logger.info("Window mode exited")

    def _init_consumer_groups_widget(self):
        self.logger.info("Initializing consumer groups status display ")
        self.consumer_groups = self.monitor_window.add(MultiLineActionWidget,
                                                       name="Consumer Groups",
                                                       relx=1,
                                                        rely=2,
                                               max_height=int(23*self.Y_SCALING_FACTOR),
                                               max_width=int(100*self.X_SCALING_FACTOR)
                                               )

        self.consumer_groups.entry_widget.values = ["Retrieving data ..."]
        self.consumer_groups.entry_widget.scroll_exit = False


    def _init_helper_commands_widget(self):
        self.commands = self.monitor_window.add(npyscreen.FixedText,
                                        relx=1,
                                        rely=int(24*self.Y_SCALING_FACTOR),
                                        name="Commands"
                                       )
        self.commands.value = "^K : Kill  ^g : top    q : quit"
        self.commands.display()
        self.commands.editable = False

