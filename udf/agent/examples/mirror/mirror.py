import sys
import json
from kapacitor.udf.agent import Agent, Handler, Server
from kapacitor.udf import udf_pb2
import signal

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


# Mirrors all points it receives back to Kapacitor
class MirrorHandler(Handler):
    def __init__(self, agent):
        self._agent = agent


    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.STREAM
        response.info.provides = udf_pb2.STREAM
        return response

    def init(self, init_req):
        response = udf_pb2.Response()
        response.init.success = True
        return response

    def snapshot(self):
        response = udf_pb2.Response()
        response.snapshot.snapshot = ''
        return response

    def restore(self, restore_req):
        response = udf_pb2.Response()
        response.restore.success = False
        response.restore.error = 'not implemented'
        return response

    def begin_batch(self, begin_req):
        raise Exception("not supported")

    def point(self, point):
        response = udf_pb2.Response()
        response.point.CopyFrom(point)
        self._agent.write_response(response, True)

    def end_batch(self, end_req):
        raise Exception("not supported")

class accepter(object):
    _count = 0
    def accept(self, conn, addr):
        self._count += 1
        a = Agent(conn, conn)
        h = MirrorHandler(a)
        a.handler = h

        logger.info("Starting Agent for connection %d", self._count)
        a.start()
        a.wait()
        logger.info("Agent finished connection %d",self._count)

if __name__ == '__main__':
    path = "/tmp/mirror.sock"
    if len(sys.argv) == 2:
        path = sys.argv[1]
    server = Server(path, accepter())
    logger.info("Started server")
    server.serve()

