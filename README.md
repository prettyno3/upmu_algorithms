
# Quick introduction

## Uploading a new algorithm

If you are unfamiliar with the git command line interface, then I recommend editing the files directly on Github. Create a directory with your initials, and place your algorithms inside them

## Connecting to the server

The server is 'dists.cal-sdb.org' and you can connect over port 7500. If you are finding that the username and password are not working, it's probably because you forgot the port.

```
ssh -p 7500 user@dists.cal-sdb.org
```

The username and password are distributed out of band. Email m.andersen@cs.berkeley.edu if you need to know what they are.

## Running an algorithm

On the server, go into ~upmu/alg/ and execute your desired algorithm. You can use GNU screen to detach and leave it running if required.

## Killing a rampant algorithm

On occasion, a bug in your program at just the wrong spot can cause an endless string of errors. If the normal method of aborting an algorithm (Ctrl-c) fails, you may need to stop it (Ctrl-z), then taking note of the job number, kill the job with kill -9. As an example:

```
upmu@dists:~/alg/MPA$ python example_sin.py 
we inserted new version
we inserted new version
CONNECTED TO ARCHIVER
Invoking computation
flushing stream
flushing stream
Computation done (14.825 ms)
^Z
[3]+  Stopped                 python example_sin.py
upmu@dists:~/alg/MPA$ kill -9 %3
[3]+  Stopped                 python example_sin.py
upmu@dists:~/alg/MPA$ jobs
[3]+  Killed                  python example_sin.py
```

# Functions available to the QuasarDistillate class


```Python

@staticmethod
def date(dst):
    """
    This parses a modified isodate into nanoseconds since the epoch. 
    The date format is: YYYY-MM-DDTHH:MM:SS.NNNNNNNNN
    Fields may be omitted right to left, but do not elide leading 
    or trailing zeroes in any field

    returns nanoseconds since the epoch
    """

@staticmethod
def now():
    """
    Returns the current time in nanoseconds
    """

def set_version(self, ver):
    """
    Sets the script version, can only be called from config()
    """

def set_author(self, author):
    """
    Sets the script author, can only be called from config()
    """

def set_name(self, name):
    """
    Sets the distillate name, can only be called from config()
    """

def add_stream(self, name, unit):
    """
    Add an output stream, i.e one that we are generating. Can
    only be called from config()
    """

def use_stream(self, name, uid):
    """
    declares a dependency on the given uuid, and associates
    it with the given name for use with stream_get. May only
    be called from the config() method
    """

def get_version_of_last_query(self, name):
    """
    Get the version used to last satisfy a query to the named
    stream. This is not realtime, it is obtained from the 
    dependency list in the metadata which is only updated after
    the previous compute() call
    """
        
def stream_delete_range(self, name, start, end):
    """
    Delete from start time (incl.) to end time (non incl.) in
    the named output stream

    requires the yield keyword
    """
    
def get_changed_ranges(self, names, gens):
    """
    Returns a list of (start, end) for the list of named input
    streams. gens may be a list of generations, any of which may
    be "auto", or the entire list can be replaced with "auto".
    The auto keyword essentially calls get_version_of_last_query()
    for the named stream.

    requires the yield keyword
    """

def persist(self, name, value):
    """
    stores value, associated with the key name
    """

def unpersist(self, name, default):
    """
    return the persisted value for name, or the given default
    if no value has been persisted for that name and the current
    script version
    """

def stream_get(self, name, start, end):
    """
    gets a list of records from the named stream between the
    given start and end times.

    requires the yield keyword
    """

def stream_flush(self, name):
    """
    make sure that any pending writes to the given stream
    are written out. This is done automatically at the end of
    the compute method

    requires the yield keyword
    """

def stream_insert(self, name, time, value):
    """
    Insert a single time, value pair into the named stream.
    This is much slower than batching and then calling
    stream_insert_multiple()

    requires the yield keyword
    """

def stream_insert_multiple(self, name, values):
    """
    Insert the given list of (time, value) pairs into the
    named stream

    requires the yield keyword
    """

```

In addition, the qdf package has the following constants, which may be useful:
```
OPTIMAL_BATCH_SIZE  # The best number of records to process per insert chunk
MICROSECOND         # The number of nanoseconds in a microsecond
MILLISECOND         # The number of nanoseconds in a millisecond
SECOND              # The number of nanoseconds in a second
MINUTE              # The number of nanoseconds in a minute
HOUR                # The number of nanoseconds in an hour
DAY                 # The number of nanoseconds in a day
```


