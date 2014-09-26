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
        self.input_name = "L1MAG"
        input_uid = "abffcf07-9e17-404a-98c3-ea4d60042ff3"
        version = 3

        #This is the first level in the distillate tree
        self.set_author("CAB")

        #This is the second level. This name should be unique for every algorithm you write
        self.set_name("Scale_Amp")

        #This is the final level. You can have multiple of these
        self.add_stream("soda_a_L1Mag_4.5", unit="V")

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
        start_date = self.date("2014-09-07T00:00:00.000000")
        end_date = self.date("2014-09-07T06:00:00.000000")
        scale_factor = 4.5

        input_version, input_values = yield self.stream_get(self.input_name, start_date, end_date)
        scaled_values = []

        idx = 0
        while idx < len(input_values):
            scaled_value = input_values[idx].value * scale_factor
            scaled_values.append((input_values[idx].time, scaled_value))
            if len(scaled_values) >= qdf.OPTIMAL_BATCH_SIZE:
                yield self.stream_insert_multiple("soda_a_L1Mag_4.5", scaled_values)
                scaled_values = []
            idx += 1

        yield self.stream_insert_multiple("soda_a_L1Mag_4.5", scaled_values)

        #Now that we are done, save the time we finished at
        self.persist("done", True)


qdf.register(ExampleScale())
qdf.begin()
