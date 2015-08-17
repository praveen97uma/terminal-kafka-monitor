import threading
import sys,os
import logging

log_file = "/tmp/kafka_monitor.log"


logger = logging.getLogger('KafkaMonitor')
handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from manager import KafkaMonitor 

def run_kafka_monitor():
    try:
        logger.info("Init stop event")
        global_stop_event = threading.Event()
        logger.info("Init GUI app")
        TestApp = KafkaMonitor(global_stop_event)
        logger.info("Running GUI")
        TestApp.run()
    except KeyboardInterrupt:
        global_stop_event.set()
        raise SystemExit   


if __name__ == '__main__':
    run_kafka_monitor()
