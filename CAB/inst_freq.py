__author__ = 'immesys'

import numpy as np
import qdf
from twisted.internet import defer

class Inst_Freq(qdf.QuasarDistillate):

    def setup(self, opts):
        """
        This constructs your distillate algorithm
        """
        #TEMP. In future, find dynamically
        self.input_stream = "C1ANG"
        self.output_stream = "soda_a_C1ANG"
        input_uid = "888b8f61-c2a4-44a1-bd5c-9865ea6ea8ca"
        version = 1

        #This is the first level in the distillate tree
        self.set_author("CAB")

        #This is the second level. This name should be unique for every algorithm you write
        self.set_name("Instantaneous Frequency")

        #This is the final level. You can have multiple of these
        self.add_stream(self.output_stream, unit="deg/s")

        self.use_stream(self.input_stream, input_uid)

        #If this is incremented, it is assumed that the whole distillate is invalidated, and it
        #will be deleted and discarded. In addition all 'persist' data will be removed
        self.set_version(version)

    @defer.inlineCallbacks
    def compute(self):
        """
        This is called to compute your algorithm.

        This example generates the difference between two streams
        """

        if self.unpersist("done",False):
            print "Already done"
            return

        #TEMP. In future, find dynamically
        start_date = self.date("2014-08-17T00:00:00.000000")
        end_date = self.date("2014-08-17T00:15:00.000000")

        input_version, input_phases = yield self.stream_get(self.input_stream, start_date, end_date)
        inst_freqs = []

        i = 0
        while i < len(input_phases)-1:
            phase_diff = input_phases[i+1].value - input_phases[i].value
            delta = input_phases[i+1].time - input_phases[i].time
            if delta == 0:
                i += 1
                continue
            if phase_diff > 180:
                phase_diff -= 360
            elif phase_diff < -180:
                phase_diff += 360
            inst_freqs.append((input_phases[i].time, (phase_diff/delta)*1e9))
            if len(inst_freqs) >= qdf.OPTIMAL_BATCH_SIZE:
                yield self.stream_insert_multiple(self.output_stream, inst_freqs)
                inst_freqs = []
            i += 1

        yield self.stream_insert_multiple(self.output_stream, inst_freqs)

        #Now that we are done, save the time we finished at
        self.persist("done", True)


qdf.register(Inst_Freq())
qdf.begin()
