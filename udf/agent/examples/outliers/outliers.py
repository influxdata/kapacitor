import sys
import json
from kapacitor.udf.agent import Agent, Handler
from kapacitor.udf import udf_pb2

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


# Find outliers via the Tukey method. As defined in the README.md
class OutliersHandler(Handler):

    class state(object):
        def __init__(self):
            self._entries = []

        def reset(self):
            self._entries = []

        def update(self, value, point):
            self._entries.append((value, point))

        def outliers(self, scale):
            first, third, lower, upper = self.bounds(scale)
            outliers = []

            # Append lower outliers
            for i in range(first):
                if self._entries[i][0] < lower:
                    outliers.append(self._entries[i][1])
                else:
                    break
            # Append upper outliers
            for i in range(third+1, len(self._entries)):
                if self._entries[i][0] > upper:
                    outliers.append((self._entries[i][1]))

            return outliers

        def bounds(self, scale):
            self._entries = sorted(self._entries, key=lambda x: x[0])
            ml, mr, _ = self.median(self._entries)
            _, first, fq = self.median(self._entries[:mr])
            third, _, tq = self.median(self._entries[ml+1:])
            iqr = tq - fq
            lower = fq - iqr*scale
            upper = tq + iqr*scale
            return first, third, lower, upper

        def median(self, data):
            l = len(data)
            m = l / 2
            if l%2 == 0:
                left = m
                right = m + 1
                median = (data[left][0]+ data[right][0]) / 2.0
            else:
                left = m
                right = m
                median = data[m][0]
            return left, right, median


    def __init__(self, agent):
        self._agent = agent
        self._field = None
        self._scale = 1.5
        self._state = OutliersHandler.state()
        self._begin_response = None


    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.BATCH
        response.info.provides = udf_pb2.BATCH
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        response.info.options['scale'].valueTypes.append(udf_pb2.DOUBLE)

        logger.info("info")
        return response

    def init(self, init_req):
        success = True
        msg = ''
        for opt in init_req.options:
            if opt.name == 'field':
                self._field = opt.values[0].stringValue
            elif opt.name == 'scale':
                self._scale = opt.values[0].doubleValue

        if self._field is None:
            success = False
            msg += ' must supply field name'
        if self._scale < 1.0:
            success = False
            msg += ' invalid scale must be >= 1.0'

        response = udf_pb2.Response()
        response.init.success = success
        response.init.error = msg[1:]

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
        self._state.reset()

        # Keep copy of begin_batch
        response = udf_pb2.Response()
        response.begin.CopyFrom(begin_req)
        self._begin_response = response

    def point(self, point):
        value = point.fieldsDouble[self._field]
        self._state.update(value, point)

    def end_batch(self, end_req):
        # Get outliers
        outliers = self._state.outliers(self._scale)

        # Send begin batch with count of outliers
        self._begin_response.begin.size = len(outliers)
        self._agent.write_response(self._begin_response)

        response = udf_pb2.Response()
        for outlier in outliers:
            response.point.CopyFrom(outlier)
            self._agent.write_response(response)

        # Send an identical end batch back to Kapacitor
        response.end.CopyFrom(end_req)
        self._agent.write_response(response)


if __name__ == '__main__':
    a = Agent()
    h = OutliersHandler(a)
    a.handler = h

    logger.info("Starting Agent")
    a.start()
    a.wait()
    logger.info("Agent finished")

