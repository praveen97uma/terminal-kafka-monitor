import subprocess
import threading
import logging


from collections import namedtuple, defaultdict


ZOOKEEPER_HOST = 'he-zookeeper-vpc.hackerearth.com:2181'


BIN_PATH = '/home/praveen/kafka/kafka_2.9.2-0.8.1.1/bin'


CONSUMER_OFFSET_CHECKER_CMD = ' '.join([
    '{bin_path}/kafka-run-class.sh',
    'kafka.tools.ConsumerOffsetChecker',
    '--zkconnect=he-zookeeper-vpc.hackerearth.com:2181',
    '--group={group_name}',
    ])

group_name = 'RequestLogMessagesToMongoUploader'


ConsumerOffsetInfo = namedtuple('ConsumerOffsetInfo', 'group topic pid offset logsize lag owner')


logger = logging.getLogger("KafkaMonitor")


class ConsumerOffsetInfoWorker(threading.Thread):
    def __init__(self, shared, group, stop_event=None,  *args, **kwargs):
        super(ConsumerOffsetInfoWorker, self).__init__(*args, **kwargs)
        self.shared = shared
        self.group = group
        self.stop_event = stop_event

    def run(self):
        #if self.stop_event:
        #    while not self.stop_event.wait(0.1):
        #        self._get_offsets()
        #else:
        #    self._get_offsets()
        self._get_offsets()
    
    def _get_offsets(self):
        offsets = get_offsets_for_group(self.group)
        for info in offsets:
            self.shared[info.group].append(info)


def get_offsets_for_group(group):
    """Retrieves the offsets for the given Kafka consumer group.
    """
    cmd = CONSUMER_OFFSET_CHECKER_CMD.format(group_name=group,
            bin_path=BIN_PATH)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    out, err = p.communicate()

    header_start_index = out.find("Group")
    output_of_concern = out[header_start_index:]

    lines = output_of_concern.split("\n")
    header = lines[0]
    consumer_offset_info = lines[1:]

    info = []
    for line in consumer_offset_info:
        data = line.split()
        if data.__len__() != 7:
            continue
        group = data[0]
        topic = data[1]
        pid = data[2]
        offset = data[3]
        logsize = data[4]
        lag = data[5]
        owner = data[6]

        offset_info = ConsumerOffsetInfo(group=group, topic=topic,
                                        pid=pid, offset=offset,
                                        logsize=logsize, lag=lag, owner=owner)
        info.append(offset_info)
    return info



groups = [
        group_name, 
        'ViewedProfileNewsFeedJobsConsumer',
        'ConsumeCodeAnalysisQueue',
        'ConsumeFixSubmissionFileQueue',
        'InsightsProblemSubmissionQueue',
        'ConsumeUpdatePracticeProblems']

def get_consumer_groups_offset_status(groups=groups, stop_event=None):
    """Retrieve consumer offsets for the given set of
    Kafka consumer groups.
    """
    shared = defaultdict(list)
    workers = [ConsumerOffsetInfoWorker(shared, group_name,
              stop_event=stop_event) for group_name in
              groups]

    for worker in workers:
        worker.start()

    for th in workers:
        th.join()
    results = []
    for group_name, partition_info in shared.iteritems():
        for group_info in partition_info:
            result = '{topic: <30} {group: <60} {pid: <2} {logsize: <13} {offset: <13} {lag: <13}'.format(group=group_info.group,
                          topic=group_info.topic,
                          pid=group_info.pid,
                          offset=group_info.offset,
                          logsize=group_info.logsize,
                          lag=group_info.lag,
                          owner=group_info.owner)
            results.append(result)
    logger.debug("Results found")
    logger.debug(results)
    return results
