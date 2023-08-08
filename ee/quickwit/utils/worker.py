from multiprocessing import Pool, Process, Pipe, TimeoutError
from multiprocessing.connection import Connection
from utils.signal_handler import signal_handler
from utils.uploader import quickwit_ingest
from confluent_kafka import Consumer
from decouple import config
#from queue import Queue
from time import time
import json


def message_type(message):
    if 'loaded' in message.keys():
        return 'pageevent'
    elif 'variables' in message.keys():
        return 'graphql'
    elif 'status' in message.keys():
        return 'fetchevent'
    else:
        raise Exception('Message form not recognized')


def kafka_reader_task(pipe: Connection):
    kafka_sources = config('KAFKA_SERVER')
    kafka_params = {
        "security.protocol": "SSL",
        "bootstrap.servers": kafka_sources,
        "group.id": config("group_id"),
        "auto.offset.reset": "earliest",
        # value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        "enable.auto.commit": False
    }
    kafka_consumer = Consumer(kafka_params)
    max_messages = config('MAX_READ_SIZE', default=600000, cast=int)
    max_read_time = config('MAX_READ_TIME', default=10, cast=int)
    while True:
        n_messages = 0
        start_time = time()
        splitted_messages = {
            'fetchevent': list(),
            'graphql': list(),
            'pageevent': list()
         }
        while (n_messages < max_messages and time() - start_time < max_read_time):
            msg = kafka_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'[Consumer error] {msg.error()}')
                continue
            value = json.loads(msg.value().decode('utf-8'))
            try:
                key = message_type(value)
                splitted_messages[key].append(value)
                n_messages += 1
            except Exception as e:
                print(f'[WARN] {e}')
        pipe.send(splitted_messages)
        action = pipe.recv()
        if action == 'CLOSE':
            break
        kafka_consumer.commit()
    kafka_consumer.unsubscribe()
    kafka_consumer.terminate()


class WorkerPool:
    pool: Pool
    n_worker: int
    queue: dict[str, list]

    def __init__(self):
        self.n_workers = config('NUMBER_OF_WORKERS', default=3, cast=int)
        fetchevent_maxsize = config('fetch_maxsize', default=100, cast=int)
        graphql_maxsize = config('graphql_maxsize', default=100, cast=int)
        pageevent_maxsize = config('pageevent_maxsize', default=100, cast=int)
        self.pool = Pool(self.n_workers)
        self.status = {
            'fetchevent': [0, fetchevent_maxsize],
            'graphql': [0, graphql_maxsize],
            'pageevent': [0, pageevent_maxsize]
        }
        self.queues = {
            'fetchevent': list(),
            'graphql': list(),
            'pageevent': list()
        }

    def run(self):
        main_conn, reader_conn = Pipe()
        reader_process = Process(target=kafka_reader_task, args=(reader_conn,))
        reader_process.start()
        while signal_handler.KEEP_PROCESSING:
            messages = main_conn.recv()
            upload_memory = False
            for message_key in messages.keys():
                self.queues[message_key] += messages[message_key]
                self.status[message_key][0] += len(messages[message_key])
                if self.status[message_key][0] >= self.status[message_key][1]:
                    upload_memory = True
            if upload_memory:
                async_tasks = list()
                for message_key, message_list in self.queues.items():
                    if len(message_list) == 0:
                        continue
                    async_tasks.append(self.pool.apply_async(quickwit_ingest, args=(message_key, message_list)))
                results = list()
                for async_task in async_tasks:
                    try:
                        results.append(async_task.get(timeout=5))
                    except TimeoutError:
                        print('[error] timeout error while uploading')
                    except Exception as e:
                        print(repr(e))
                for event_key in self.status.keys():
                    self.status[event_key][0] = 0
                    self.queues[event_key] = list()

            main_conn.send('CONTINUE')
        main_conn.send('CLOSE')

