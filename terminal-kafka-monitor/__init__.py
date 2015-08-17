import sys,os
import logging

log_file = "/tmp/kafka_monitor.log"


logger = logging.getLogger('KafkaMonitor')
handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
