
import sys
import json
from agent import Agent, Handler
import udf_pb2

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger()


# Computes the moving average of the data it receives.
# The options it has are:
#    size - the number of data points to keep in the window
#    field - the field to operate on
#    as - the name of the average field, default 'avg'
#
class AvgHandler(Handler):

    class state(object):
        def __init__(self):
            self.size = 0
            self._window = []
            self._avg = 0.0

        def update(self, value):
            l = float(len(self._window))

            if l == self.size:
                # Window is full, just iteratively update the avg
                self._avg += value/l - self._window[0]/l
                self._window.pop(0)
            else:
                # Window is not full compute the cumulative avg
                self._avg = (value + l*self._avg) / (l + 1)

            self._window.append(value)
            return self._avg



        def snapshot(self):
            data = {
                    'size' : self.size,
                    'window' : self._window,
                    'avg' : self._avg,
            }
            return json.dumps(data)

        def restore(self, snapshot):
            data = json.loads(snapshot)
            self.size = int(data['size'])
            self._window = [float(d) for d in data['window']]
            self._avg = float(data['avg'])

    def __init__(self, agent):
        self._agent = agent
        self._field = None
        self._as = 'avg'
        self._state = AvgHandler.state()


    def info(self):
        response = udf_pb2.Response()
        response.info.wants = udf_pb2.STREAM
        response.info.provides = udf_pb2.STREAM
        response.info.options['field'].valueTypes.append(udf_pb2.STRING)
        response.info.options['size'].valueTypes.append(udf_pb2.INT)
        response.info.options['as'].valueTypes.append(udf_pb2.STRING)

        logger.info("info")
        return response

    def init(self, init_req):
        success = True
        msg = ''
        for opt in init_req.options:
            if opt.name == 'field':
                self._field = opt.values[0].stringValue
            elif opt.name == 'size':
                self._state.size = opt.values[0].intValue
            elif opt.name == 'as':
                self._as = opt.values[0].stringValue

        if self._field is None:
            success = False
            msg += ' must supply field name'
        if self._state.size == 0:
            success = False
            msg += ' must supply window size'
        if self._as == '':
            success = False
            msg += ' invalid as name'

        response = udf_pb2.Response()
        response.init.success = success
        response.init.error = msg[1:]

        return response

    def snapshot(self):
        data = self._state.snapshot()
        response = udf_pb2.Response()
        response.snapshot.snapshot = data

        return response

    def restore(self, restore_req):
        success = False
        msg = ''
        try:
            self._state.restore(restore_req.snapshot)
            success = True
        except Exception as e:
            success = False
            msg = str(e)

        response = udf_pb2.Response()
        response.restore.success = success
        response.restore.error = msg

        return response

    def begin_batch(self):
        raise Exception("not supported")

    def point(self, point):
        response = udf_pb2.Response()
        response.point.time = point.time
        response.point.name = point.name
        response.point.database = point.database
        response.point.retentionPolicy = point.retentionPolicy
        response.point.group =  point.group
        response.point.dimensions.extend(point.dimensions)
        response.point.tags.update(point.tags)

        value = point.fieldsDouble[self._field]
        avg = self._state.update(value)
        response.point.fieldsDouble[self._as] = avg
        self._agent.write_response(response)

    def end_batch(self, end_req):
        raise Exception("not supported")


if __name__ == '__main__':
    a = Agent()
    h = AvgHandler(a)
    a.handler = h

    logger.info("Starting Agent")
    a.start()
    a.wait()
    logger.info("Agent finished")

