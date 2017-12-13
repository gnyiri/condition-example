import threading
import time
import logging
from queue import Queue

logging.basicConfig(level=logging.INFO, format='%(relativeCreated)6d %(threadName)s %(message)s')


class AsyncLogWorker(threading.Thread):
    """
    Log worker: does a time-consuming work
    """
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.do_run = True
        logging.debug('Start worker')

    def run(self):
        while self.do_run:
            # waiting for wake-up signal
            with self.parent.MESSAGE_CONDITION:
                logging.debug('Waiting for notification..')
                self.parent.MESSAGE_CONDITION.wait()

            logging.debug('Notification arrived!')
            # process items from queue
            while not self.parent.MESSAGE_QUEUE.empty():
                item = self.parent.MESSAGE_QUEUE.get()
                logging.info('Process %s' % item)
                time.sleep(0.25)


class AsyncLogger(object):
    """
    Standard output non-blocking logger, flushes log messages periodically
    """
    # log message buffer
    MESSAGE_QUEUE = Queue()
    # condition variable
    MESSAGE_CONDITION = threading.Condition()
    # number of workers
    NUM_WORKERS = 4

    def __init__(self):
        super().__init__()
        # start log writer thread
        self.log_writers = []
        for w in range(self.NUM_WORKERS):
            worker = AsyncLogWorker(self)
            worker.start()
            self.log_writers.append(worker)

    def log(self, msg: str):
        logging.debug('Enqueue %s', msg)
        # put message into queue
        self.MESSAGE_QUEUE.put_nowait(msg)
        # send wake-up signal to on worker
        with self.MESSAGE_CONDITION:
            logging.debug('Notify one worker..')
            self.MESSAGE_CONDITION.notify()

    def stop(self):
        for w in self.log_writers:
            w.do_run = False

        with self.MESSAGE_CONDITION:
            self.MESSAGE_CONDITION.notify_all()

        for w in self.log_writers:
            w.join()


if __name__ == '__main__':
    async_logger = AsyncLogger()
    for i in range(25):
        async_logger.log("%s (%d)" % (str(i), i))
        time.sleep(0.025)

    time.sleep(1)

    for i in range(25, 50):
        async_logger.log("%s (%d)" % (str(i), i))
        time.sleep(0.025)

    time.sleep(10)
    async_logger.stop()
