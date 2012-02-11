import sys
import os

sys.path.insert(0, os.path.dirname(__file__) + '/../src')

from tempest import Cluster

app = Cluster()
worker = app.worker('sinatra')

@worker.on('url')
def url(context, env):
    body = "Hello world\n"
    return 200, {
        'Content-Type': 'text/html;charset=utf-8',
        'Content-length': len(body)}, body

worker.run()


