import redis as redislib
import  json

__clients = {}

def client(name):
    if not __clients.has_key(name):
        host, port = name.split(':')
        port = int(port)
        __clients[name] = redislib.StrictRedis(host=host, port=port, db=0)
    return __clients[name]


class Context(object):

    def __init__(self, worker, answer, job_id):
        self.worker = worker
        self.answer = answer
        self.job_id = job_id

    def stop(self):
        self.worker.loop = False


class WorkProxy(object):

    def __init__(self, worker):
        self._worker = worker

    def __getattr__(self, name):
        def w(answer, *args):
            self._worker.cluster.id += 1
            client(self._worker.cluster.redis).rpush(
                self._worker.queue, json.dumps(
                    [name, args, answer, self._worker.cluster.id]))
        return w


class Worker(object):

    def __init__(self, cluster, queue):
        self.cluster = cluster
        self.queue = queue
        self._on = {}
        self.loop = False
        self.work = WorkProxy(self)

    def on(self, cmd):
        def routine(action):
            self._on[cmd] = action
        return routine

    def run(self):
        self.loop = True
        while self.loop:
            _, task = client(self.cluster.redis).blpop(self.queue, 0)
            cmd, args, answer, job_id = json.loads(task)
            resp = self._on[cmd](Context(self, answer, job_id), *args)
            if answer and resp is not None:
                if type(resp) is tuple:
                    resp = list(resp)
                if type(resp) is not list:
                    resp = [resp]
                client(answer).execute_command(cmd, job_id, json.dumps(resp))


class Cluster(object):

    def __init__(self, redis='localhost:6379'):
        self.redis = redis
        self.id = 0

    def worker(self, queue):
        return Worker(self, queue)
