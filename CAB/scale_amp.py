__author__ = 'immesys'

import numpy as np
import qdf
from twisted.internet import defer

class ExampleScale(qdf.QuasarDistillate):

    def setup(self, opts):
        """
        This constructs your distillate algorithm
        """
        #TEMP. In future, find dynamically
        self.input_name = "1hz"
        input_uid = "2942433b-6511-4298-a0b2-2886456cff4f"
        version = 1

        #This is the first level in the distillate tree
        self.set_author("CAB")

        #This is the second level. This name should be unique for every algorithm you write
        self.set_name("Scale_Amp")

        #This is the final level. You can have multiple of these
        self.add_stream("Scale_Amp", unit="Amp_Unit")

        self.use_stream(self.input_name, input_uid)

        #If this is incremented, it is assumed that the whole distillate is invalidated, and it
        #will be deleted and discarded. In addition all 'persist' data will be removed
        self.set_version(version)

    @defer.inlineCallbacks
    def compute(self):
        """
        This is called to compute your algorithm.

        This example scales an input stream by a constant factor
        """

        if self.unpersist("done",False):
            print "Already done"
            return

        #TEMP. In future, find dynamically
        start_date = self.date("2014-08-17T00:00:00.000000")
        end_date = self.date("2014-08-17T00:15:00.000000")
        scale_factor = 2.5

        input_version, input_values = yield self.stream_get(self.input_name, start_date, end_date)
        scaled_values = []

        idx = 0
        while idx < len(input_values):
            scaled_value = input_values[idx].value * scale_factor
            scaled_values.append((input_values[idx].time, scaled_value))
            if len(scaled_values) >= qdf.OPTIMAL_BATCH_SIZE:
                yield self.stream_insert_multiple(self.input_name, scaled_values)
                scaled_values = []
            idx += 1

        yield self.stream_insert_multiple(self.input_name, scaled_values)

        #Now that we are done, save the time we finished at
        self.persist("done", True)


qdf.register(ExampleScale())
qdf.begin()
