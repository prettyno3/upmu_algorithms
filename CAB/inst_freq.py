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
        version = 4

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

        This example generates the instantaneous frequency of phase
        """

        if self.unpersist("done",False):
            print "Already done"
            return

        #TEMP. In future, find dynamically
        start_date = self.date("2014-09-17T00:00:00.000000")
        end_date = self.date("2014-09-17T00:30:00.000000")

        input_version, input_phases = yield self.stream_get(self.input_stream, start_date, end_date)
        inst_freqs = []
        sampling_freq = 160 #Hz

        i = 0
        while i < len(input_phases)-sampling_freq:
            delta_samples = sampling_freq #upper bound
            t1 = input_phases[i].time
            t2 = input_phases[i+delta_samples].time
            while ((t2 - t1) > 1e9 and t2 > t1): #catch zeroed or missing samples
                delta_samples -= 1 #decrement ~one sample per missing sample in interval
                t2 = input_phases[i+delta_samples].time
            x1 = input_phases[i].value
            x2 = input_phases[i+delta_samples].value
            phase_diff = x2 - x1
            delta_time = t2 - t1
            if delta_time == 0:
                i += 1
                continue
            if phase_diff > 180:
                phase_diff -= 360
            elif phase_diff < -180:
                phase_diff += 360
            inst_freqs.append((t1, (phase_diff/delta_time)*1e9))
            if len(inst_freqs) >= qdf.OPTIMAL_BATCH_SIZE:
                yield self.stream_insert_multiple(self.output_stream, inst_freqs)
                inst_freqs = []
            i += 1

        yield self.stream_insert_multiple(self.output_stream, inst_freqs)

        #Now that we are done, save the time we finished at
        self.persist("done", True)


qdf.register(Inst_Freq())
qdf.begin()
