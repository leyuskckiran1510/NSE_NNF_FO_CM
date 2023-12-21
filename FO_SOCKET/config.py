TRADER_ID = 37055
USER_ID = 37055
PASSWORD = "password"
BROKER_ID = "12345"
HOST = "127.0.0.0"
PORT = 10850
DELAY_DURATION = 0
RESUME_DOWNLOAD = True  # continue from last left position
EXIT_ON_LOGIN_FAIL = True
SEGMENT_NAME = "FO"
STACK_SIZE = 5000  # numbers of drop copy in buffer before writing to file (THREADSHOLD FOR WRITING IN FILE)
VAR_LOGGING = False  # True to log and False to not log
SOCKET_TIMEOUT = 6  # socket timeout in seconds
STACK_TIMER = 5  # seconds
NOTIS_FORMAT = True  # if it is true then output will be on notis format else in dropcopy format
GIVE_UP_SAVE = True  # make it false to don't save giveup responses
DC_FORMAT = True
RECIVE_SIZE = 4096  # this handels how much data chunk to process at once read (1) for more info
LIMIT = 4_000  # this is the limit of data has to keep track to avoid duplicates if any , read (2) for more

"""
1)  SEVER TCP buffer/bucket, [total amount of bytes a system can hold before throwing the data ]
      4096[default]    131072[medium]  6291456[max]

2) Due to loading of huge chunks now, the data we recive are more likely to be redundent and have to keep track of last X/N number
    of data hash to make sure we have not saved the same data twice,
    Here the value of `LIMIT` denotes the amount of DCs  to be kept of last saved copies of DC, and if new DC has same as the 
        recored ; then ignore it, For now last 4_000 is enogh
"""
