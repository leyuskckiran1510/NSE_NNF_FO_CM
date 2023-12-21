import os
import time
import socket
import struct
import hashlib
import logging
import datetime
import traceback
from config import *
from threading import Thread


TIMEN = time.strftime("%Y%m%d")

TotalModules = 0
EXIT_FLAG = False
TOTAL_RECORDS = 0
RECORD_TIME = {1: datetime.datetime(1980, 1, 1, 0, 0, 0)}
STACK = []
PROGRAM_TIMER = time.time()
ALL_TIMESTAMP:set = set()
CORRUPTION = False
BOOM = True

logging.basicConfig(
    filename=f"Log_{TIMEN}.log",
    format="%(asctime)s %(message)s",
    filemode="a",
)
logger = logging.getLogger()
if VAR_LOGGING:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.ERROR)


CSV_FILE_HEADER = "ResOrdNum,BrkNum,TraderNum,AccountNum,BuySell,OrginalVol,DisclosedVol,RemainingVol,RemainingDisclsVol,Price,STORDERFLAGS,Gtd,FillNum,FillQty,FillPrice,VolFilledToday,ActicityTyp,ActicityTime,OpOrdNumber,OpBrkId,Token,ContractSection,OpenClose,OldOpenClose,BookType,NewVol,OldAccountNum,Participant,ParticipantOld,ADDORDERFLAGS,GiveUpTrade,Pan,OldPan,AlgoId,AlgoCategory,LstAcitvityRefrence,NnfField"
GIVE_UP_HEADERS = "ReasonCode,OrderNumber,FillNumber,InstrumentName,Symbol,ExpiryDate,StrikePrice,OptionType,CALevel,FillVolume,FillPrice,BrokerId,BuySell,whether,BookType,LastModifiedDateTime,InitiatedByControl,OpenClose,Participant,GiveupFlag,Deleted"

if NOTIS_FORMAT:
    '''
    How SAVE_DATA_CHOICES variable works

    choose which index of data are to be dumped from the orginal CSV_FILE_HEADER
    # Example:-
    SAVE_DATA_CHOICES = [0,1,2,3,]
    will save data with headers:- ResponseOrderNumber,BrokerNumber,TraderNum,AccountNUM
    the SAVE_DATA_CHOICES holds the indexes of the data to be saved into files and it can be changed to make small chunks of data on your own
    
    # More Examples:-
    
    IF you only want the BrokerNumber,BuySell then you can do such
    
    SAVE_DATA_CHOICES = [1,4] 
    
    this will only save BrokerNumber and BuySell into a file
     
     
     
    '''
    FORMAT_NAME = "NOTIS"
    SAVE_DATA_CHOICES = [0,1,2,3,]
else:
    FORMAT_NAME = " DC"
    SAVE_DATA_CHOICES = list(range(0,len(CSV_FILE_HEADER.split(','))))
def strftimesec(x) -> str:
    gps_epoch = datetime.datetime(1980, 1, 1, 0, 0, 0)
    utc_datetime = gps_epoch + datetime.timedelta(seconds=x)
    return utc_datetime


def write_drop_copy(save_csv="", on_exit=False):
    global STACK, PROGRAM_TIMER, TOTAL_RECORDS,BOOM
    if save_csv not in STACK:
        STACK.append(save_csv)
        TOTAL_RECORDS += 1
    else:
        print("Skipping Duplicates",end="\r")
    
    if (
        len(STACK) >= STACK_SIZE
        or on_exit
        or (STACK_TIMER <= (time.time() - PROGRAM_TIMER))
    ):
        with open(
            f"TRADE_{SEGMENT_NAME}_{FORMAT_NAME}_{TIMEN}.csv",
            "a",
        ) as fl:
            if TOTAL_RECORDS > 1 and not on_exit:
                fl.write("\n")
            fl.write("\n".join([i for i in STACK if i]))
            # BOOM = False
        PROGRAM_TIMER = time.time()
        STACK = []


class Errors:
    dic = {
        16001: "ERR_INVALID_USER_TYPE",
        16003: "ERR BAD TRANSACTION CODE",
        16004: "ERR_USER _ALREADY_SIGNED_ON",
        16006: "ERR_INVALID _SIGNON",
        16007: "ERR_SIGNON_NOT_POSSIBLE",
        16041: "ERR INVALID BROKER OR BRANCH",
        16042: "ERR_USER NOT FOUND",
        16056: "ERR_PROGRAM_ERROR",
        16104: "ERR_SYSTEM_ERROR",
        16123: "ERR_CANT_COMPLETE_YOUR_REQUEST",
        16134: "ERR_USER IS_DISABLED",
        16148: "ERR_INVALID_USER_ID",
        16154: "ERR_INVALID_TRADER_ID",
        16285: "ERR_BROKER_NOT_ACTIVE",
    }


class TransactionCode:
    dic = {
        2300: "SIGNON_IN(login requests [Client -> Host])",
        2301: "SIGNON_OUT(loginsucees [Host -> Client])",
        8000: "DROP_COPY_MESSAGE_DOWNLOAD(requests [Client -> Host])",
        2222: "TRADE_CONFIRMATION(drop_copy_response_sucess [Client -> Host])",
        2282: "TRADE_CANCEL_CONFIRM [Host -> Client]",
        2286: "TRADE_CANCEL_REJECT [Host -> Client]",
        2287: "TRADE_MODIFY_CONFIRM [Host -> Client]",
        23506: "HEART_BEAT ([Client -> Host])",
    }


class ContractDesc:
    """
    InstrumentName CHAR 6 0
    Symbol CHAR 10 6
    ExpiryDate LONG 4 16
    StrikePrice LONG 4 20
    OptionType CHAR 2 24
    CALevel SHORT 2 26
    """

    def __init__(self, content: bytes):
        self.content = content
        (
            self.instrument_name,
            self.symbol,
            self.expiry_date,
            self.strike_price,
            self.option_type,
            self.CALevel,
        ) = struct.unpack("!6s10sll2sh", content)
        self.expiry_date = strftimesec(self.expiry_date)
        self.string = f"{self.instrument_name.decode().strip()} , {self.symbol.decode().strip()} , {self.expiry_date} , {self.strike_price} , {self.option_type.decode().strip()} , {self.CALevel}"


class AddOrdFlg:
    def __init__(self, content: bytes):
        self.content = content
        self.COL = bin(struct.unpack("!b", content)[0])[2:].zfill(8)[6]
        self.string = f"{bool(self.COL)}"


class StOdrFlg:
    FLAGS = [
        "ATO",
        "Market",
        "MIT",
        "Day",
        "GTC",
        "IOC",
        "AON",
        "MF",
        "MatchedInd",
        "Traded",
        "Modified",
        "Frozen",
        "PreOpen",
        "__reserved",
        "__reserved",
        "__reserved",
    ]

    def __init__(self, data: bytes):
        self.bits = [bit for bit in "".join([f"{bin(c)[2:].zfill(8)}" for c in data])]
        self.dic = {i: bool(int(j)) for i, j in zip(self.FLAGS[:-3], self.bits[:-3])}
        self.string = "|".join(
            [i for i, j in zip(self.FLAGS[:-3], self.bits[:-3]) if int(j)]
        )



class Packer:
    _sequence = 0

    def __init__(self, messagedata: bytes):
        self.length = 22 + len(messagedata)
        self.checksum = hashlib.md5(messagedata)
        self.checksum = self.checksum.digest()
        Packer._sequence += 1
        self.packet = struct.pack(
            f"!hl16s{len(messagedata)}s",
            self.length,
            Packer._sequence,
            self.checksum,
            messagedata,
        )


class MESSAGE:
    def __init__(
        self,
        TransactionCode: int,
        LogTime: int,
        AlphaChar: str,
        TraderId: int,
        ErrorCode: int,
        TimeStamp: int,
        TimeStamp1: str,
        TimeStamp2: str,
        MessageLength: int,
    ):
        self.TransactionCode = TransactionCode
        self.LogTime = LogTime
        self.AlphaChar = (
            AlphaChar if isinstance(AlphaChar, bytes) else str(AlphaChar).encode()
        )
        self.TraderId = TraderId
        self.ErrorCode = ErrorCode
        self.TimeStamp = TimeStamp
        self.TimeStamp1 = (
            TimeStamp1 if isinstance(TimeStamp1, bytes) else str(TimeStamp1).encode()
        )
        self.TimeStamp2 = (
            TimeStamp2 if isinstance(TimeStamp2, bytes) else str(TimeStamp2).encode()
        )
        self.MessageLength = MessageLength
        self.buffer = self.pack()

    def repack(self):
        self.buffer = self.pack()

    def pack(self):
        return struct.pack(
            "!hl2slhQ8s8sh",
            self.TransactionCode,
            self.LogTime,
            self.AlphaChar,
            self.TraderId,
            self.ErrorCode,
            self.TimeStamp,
            self.TimeStamp1,
            self.TimeStamp2,
            self.MessageLength,
        )

    @staticmethod
    def login(USER_ID, PASSWORD, BROKER_ID, TRADER_ID) -> bytes:
        logging.info(
            f"Login Payload[Message] with datas \n\t\t([+]) userid = {USER_ID} || password = {'*'*len(PASSWORD)} || BROKER_ID = { BROKER_ID} || TraderId = { TRADER_ID}"
        )
        body = LOGIN_BODY(USER_ID, PASSWORD, BROKER_ID)
        header = MESSAGE(2300, 0, 0, TRADER_ID, 0, 0, 0, 0, 40 + len(body.buffer))
        return Packer(header.buffer + body.buffer).packet

    @staticmethod
    def dropcopy(session_header, AlphaChar) -> bytes:
        header = MESSAGE(8000, 0, AlphaChar, session_header.TraderId, 0, 0, 0, 0, 48)
        if not RESUME_DOWNLOAD:
            c = 0
        else:
            c = track_me(None, module=0)
            if len(c)==8:
                c = struct.unpack("!d",c)[0]
            else:
                c=0
        pg = struct.pack("!40sd", header.buffer, float(c))
        logging.info(
            f"DropCopy PayLoad[Message] with datas \n\t\t([+]) TraderId = {header.TraderId} || AlphaChar/ConnectionId = {header.AlphaChar} || timestamp = {float(c)}"
        )
        payload = Packer(pg).packet
        logging.info(f"DropCopy Requests {payload}")
        return payload

    @staticmethod
    def hearbeat(TRADER_ID):
        logging.info(f"HeartBeat....")
        header = MESSAGE(23506, 0, 0, TRADER_ID, 0, 0, 0, 0, 40)
        return Packer(header.buffer).packet

    @staticmethod
    def unpack(data):
        data = data.encode() if isinstance(data, str) else data
        (
            TransactionCode,
            LogTime,
            AlphaChar,
            TraderId,
            ErrorCode,
            TimeStamp,
            TimeStamp1,
            TimeStamp2,
            MessageLength,
        ) = struct.unpack("!hl2slhQ8s8sh", data)
        return MESSAGE(
            TransactionCode,
            LogTime,
            AlphaChar,
            TraderId,
            ErrorCode,
            TimeStamp,
            TimeStamp1,
            TimeStamp2,
            MessageLength,
        )

    def __str__(self):
        return f"""
        TransactionCode:- {self.TransactionCode}
        LogTime:- {self.LogTime}
        AlphaChar:- {self.AlphaChar}
        TraderId:- {self.TraderId}
        ErrorCode:- {self.ErrorCode}
        TimeStamp:- {self.TimeStamp}
        TimeStamp1:- {self.TimeStamp1}
        TimeStamp2:- {self.TimeStamp2}
        Error:- {Errors.dic.get(self.ErrorCode,"No Errors"),self.ErrorCode}
        """


class LOGIN_BODY:
    _resev8 = b"\x00" * 8
    _resev38 = b"\x00" * 38
    _resev117 = b"\x00" * 117
    _resev16 = b"\x00" * 16
    _resev4 = b"\x00"*4

    def __init__(self, userid: int, password: str, bid: str) -> None:
        self.userid = userid
        self.password = password.encode() if isinstance(password, str) else password
        self.bid = bid.encode() if isinstance(bid, str) else bid
        self.buffer = self.pack()

    def pack(self):
        # changes:-
        #  password from 8s -> 12s
        #  after password reserved 8s -> 4s 
        #  after password reserved 117s -> 119s 
        return struct.pack(
            "!l8s12s4s38s5s119s16s16s16s",
            self.userid,
            self._resev8,
            self.password,
            self._resev8,
            self._resev38,
            self.bid,
            self._resev117,
            self._resev16,
            self._resev16,
            self._resev16,
        )

    @staticmethod
    def unpack(data):
        if isinstance(data, str):
            data = data.encode()
        (
            userid,
            _resev8,
            password,
            _resev8,
            _resev38,
            bid,
            _resev117,
            _resev16,
            _resev16,
            _resev16,
        ) = struct.unpack("!l8s8s8s38s5s117s16s16s16s", data)
        return LOGIN_BODY(userid, password, bid)

    def __str__(self):
        return f"""
        UserId:- {self.userid}
        Password:-{self.password.decode()}
        BrokerId:- {self.bid.decode()}
        """


class Response:
    def __init__(self, response_bytes: bytes=None):
        logging.info(
            f"Response Content (for_analysis_if_error) \n\t\t([+]) content = {response_bytes}"
        )
        self.content = response_bytes
        self.unpack()
        self.checksum = self.check_checksum()
        self.error = self.check_errors()

    def unpack(self):
        (
            self.unp_length,
            self.unp_sequence,
            self.unp_checksum,
            self.unp_messagedata,
        ) = struct.unpack(f"!hl16s{len(self.content)-22}s", self.content)

    def check_checksum(self) -> bool:
        if self.unp_checksum != hashlib.md5(self.unp_messagedata).digest():
            logging.info(
                f"Error in Response_Class ([+]) CheckSum Failed !!! [Data is corrupted]"
            )
            return True
        return False

    def check_errors(self) -> bool:
        self.header = MESSAGE.unpack(self.unp_messagedata[:40])
        self.body = self.unp_messagedata[40:]
        err = Errors.dic.get(self.header.ErrorCode, False)
        if err:
            logging.info(f"Error in Response_Class ([+]) error  = {err}")
            print(f"\t\t!!!Error!!!\n\t{err}\n")
        return bool(err)

    def __str__(self):
        return f"{self.content}"


class DropCopy:
    def __init__(self, response_bytes: bytes, th_id: int):
        global RECORD_TIME
        self.data = list(struct.unpack(
            "!d5scl10shlllll2slllll2sld5scl28sccccl10s12s12s1sccc10s10slhqd44s",
            response_bytes
        ))

        # poping the reserved bytes/ padding bytes
        self.data.pop(2)
        self.data.pop(20)
        self.data.pop(25)
        self.data.pop(30)
        self.data.pop(31)
        self.data.pop(37)

        self.clean()

        self.save_csv = " , ".join(
            [str(self.data[i]).strip() for i in SAVE_DATA_CHOICES]
        )
        
        
        if self.data[17] > RECORD_TIME.get(
            th_id, datetime.datetime(1980, 1, 1, 0, 0, 0)
        ):
            RECORD_TIME[th_id] = self.data[17]
        self.data[17] = self.data[17].strftime("%d%b%Y").upper()
        write_drop_copy(save_csv=self.save_csv)

    def clean(self):
        self.data[0] = int(self.data[0])
        self.data[1] = int(self.data[1])
        self.data[2] = self.data[2]
        self.data[3] = self.data[3].decode("utf-8").strip()
        self.data[4] = self.data[4]
        self.data[5] = self.data[5]
        self.data[6] = self.data[6]
        self.data[7] = self.data[7]
        self.data[8] = self.data[8]
        self.data[9] = self.data[9]
        self.data[10] = StOdrFlg(self.data[10]).string
        self.data[11] = self.data[11]
        self.data[12] = self.data[12]
        self.data[13] = self.data[13]
        self.data[14] = self.data[14]
        self.data[15] = self.data[15]
        self.data[16] = self.data[16].decode("utf-8").strip()
        self.data[17] = strftimesec(self.data[17])
        self.data[18] = int(self.data[18])
        self.data[19] = self.data[19].decode("utf-8").strip()
        self.data[20] = self.data[20]
        self.data[21] = ContractDesc(self.data[21]).string
        self.data[22] = self.data[22].decode("utf-8").strip()
        self.data[23] = self.data[23].decode("utf-8").strip()
        self.data[24] = self.data[24].decode("utf-8").strip()
        self.data[25] = self.data[25]
        self.data[26] = self.data[26].decode(
                    "utf-8"
                    ).strip()
        self.data[27] = self.data[27].decode("utf-8").strip()
        self.data[28] = self.data[28].decode(
                "utf-8"
                ).strip()
        self.data[29] = AddOrdFlg(self.data[29]).string
        self.data[30] = self.data[30].decode("utf-8").strip()
        self.data[31] = self.data[31].decode("utf-8").strip()
        self.data[32] = self.data[32].decode("utf-8").strip()
        self.data[33] = self.data[33]
        self.data[34] = self.data[34]
        self.data[35] = self.data[35]
        self.data[36] = int(self.data[36])


class GiveUp:

    option_type = {
        "CE": "CALL OPTION",
        "PE": "PUT OPTION",
        "XX": "FUTURES Contract"
          }
    
    book_type = {
        1:"Regular lot order",
        2:"Special terms order",
        3:"Stop loss / MIT order"
            }
    

    def __init__(self,content) -> None:
        self.error = False
        if len(content)!=82:
            self.error = True
            return
        self.data = list(struct.unpack("!hdl6s10sll2shll5schhlccc12sccc",content))
        self.clean()

        self.save_csv = " , ".join([str(i) for i in self.data])
        with open(f"GIVE_UP_{TIMEN}.csv","a") as fp:
            fp.write(f"{self.save_csv}\n")


    def clean(self):
        self.data[0] = int(self.data[0])
        self.data[1] = int(self.data[1])
        self.data[2] = int(self.data[2])
        self.data[3] = self.data[3].decode("utf-8").strip()
        self.data[4] = self.data[4].decode("utf-8").strip()
        self.data[5] = str(strftimesec(self.data[5]))
        self.data[6] = int(self.data[6])
        self.data[7] = self.option_type.get(self.data[7].decode("utf-8").strip(),' ')
        self.data[8] = int(self.data[8])
        self.data[9] = int(self.data[9])
        self.data[10] = int(self.data[10])
        self.data[11] = self.data[11].decode("utf-8").strip()
        self.data[12] = self.data[12].decode("utf-8").strip()
        self.data[13] = int(self.data[13])
        self.data[14] = self.book_type.get(int(self.data[14]))
        self.data[15] = str(strftimesec(self.data[15]))
        self.data[16] = self.data[16].decode("utf-8").strip()
        self.data[17] = self.data[17].decode("utf-8").strip()
        self.data[18] = self.data[18].decode("utf-8").strip()
        self.data[19] = self.data[19].decode("utf-8").strip()
        self.data[20] = self.data[20].decode("utf-8").strip()
        self.data[21] = self.data[21].decode("utf-8").strip()

        self.data.pop() # poping last filler byte
        self.data.pop(-4) # poping reserved byte/padding byte
        self.data.pop(12)
        

def files_ready():
    global TOTAL_RECORDS
    logging.info("Making necessary File adjustments...")
    drop_file = f"TRADE_{SEGMENT_NAME}_{FORMAT_NAME}_{TIMEN}.csv"
    if not os.path.exists(drop_file):
        with open(drop_file, "w") as fl:
            fl.write(" , ".join([CSV_FILE_HEADER.split(',')[i] for i in SAVE_DATA_CHOICES]))
    else:
        with open(drop_file, "r") as fl:
            TOTAL_RECORDS = len(fl.readlines()) - 1
    if not os.path.exists(f"GIVE_UP_{TIMEN}.csv"):
        with open(f"GIVE_UP_{TIMEN}.csv","a") as fp:
            fl.write(" , ".join([i for i in GIVE_UP_HEADERS.split(',')]))

    logging.info("File adjustments Completed.")


def suspend():
    period = 0
    while (not EXIT_FLAG) and (period <= 5):
        time.sleep(1)
        period += 1

def track_me(timestamp: bytes = None, module=0):
    if timestamp:
        with open(f"tracker_{FORMAT_NAME}.csv", "wb") as fl:
            fl.write(timestamp)
    else:
        if os.path.exists(f"tracker_{FORMAT_NAME}.csv"):
            with open(f"tracker_{FORMAT_NAME}.csv", "rb") as fl:
                return fl.read()
        else:
            return '\x00'



def thread_func(client_socket, SESSION):
    global CORRUPTION
    module = 0
    if not os.path.exists(f"tracker_{FORMAT_NAME}.csv"):
        with open(f"tracker_{FORMAT_NAME}.csv", "wb") as fl:
            fl.write(b"\x00")
    start_time = time.time()
    last_rec_time = None
    buffer=b""
    while BOOM:
        elapsed_time = time.time() - start_time
        if elapsed_time > 29:
            print("SENDING HEART_BEAT.......", end="\n")
            logging.info("Sending HeartBeat...")
            heart_beat = MESSAGE.hearbeat(SESSION.header.TraderId)
            client_socket.send(heart_beat)
            start_time = time.time()
        try:
            response = b""
            if len(buffer)>=318:
                response = buffer[:318]
                buffer=buffer[318:]
                print("Skipping The GiveUp Responses",end="\r")
            else:
                response = client_socket.recv(318-len(buffer))

            if buffer:
                response = buffer+response
                buffer=b""
            _length = struct.unpack("!h",response[:2])[0]
            if _length!=318:
                buffer = b"\x01>\x00\x00\x00\x00"+response.split(b"\x01>\x00\x00\x00\x00")[-1]
                x = response.split(b"\x01>\x00\x00\x00\x00")[0]
                if len(x)<122 or not GIVE_UP_SAVE:
                    # if this is the case then this is the bytes of DropCopy Don't do anything to it
                    # or the user has choose to not to save giveup response
                    continue

                # The bytes in x are of GiveUp 
                give_up_handler = Response(x)
                if not give_up_handler.error and give_up_handler.header.TransactionCode == 4506:
                    last_rec_time = give_up_handler.header.TimeStamp1
                    GiveUp(give_up_handler.body)
                give_up_handler = None
                x = ''
                continue
            logging.info(f"The recived data is {response}")
            logging.info(f"The recived data is {response}")
            
        except socket.timeout:
            logging.info("SocketTimeOut {response}")
            print("[socket timeout brokes]")
            continue
        if not response:
            print("[no response broke]")
            continue
        SESSION = Response(response)
        if SESSION.error or SESSION.checksum:
            # print("[session errors] ",SESSION.error ,SESSION.checksum,end="\n")
            buffer = b""
            response =b""
            CORRUPTION = True
            continue
        elif SESSION.header.TransactionCode == 2222:
            last_rec_time = SESSION.header.TimeStamp1
            DropCopy(SESSION.body, module)
        

        else:
            print(
                "TransactionCode",
                SESSION.header.TransactionCode,
                " [+] ",
                TransactionCode.dic.get(SESSION.header.TransactionCode, "Unknown"),end="\r"
            )
            print("[Unknown treansactioncode]")
            continue
        time.sleep(DELAY_DURATION)
        
    track_me(last_rec_time, module)



def display():
    #os.system('cls' if os.name == 'nt' else 'clear')
    message = f"""Press 'ctrl+c' to exit | TotalRecords: {TOTAL_RECORDS} | Last Record Time Per Module:- """+"/".join([str(i)+'[ ' +str(RECORD_TIME[i]).split(' ')[-1].strip()+' ] ' for i in RECORD_TIME])
    print(message,end="\r")
    
def set_flag():
        global EXIT_FLAG,BOOM
        BOOM=False
        EXIT_FLAG = True

def linear(client_socket, SESSION):

    try:
        while not EXIT_FLAG and not CORRUPTION:
            thread_func(client_socket, SESSION)

    except Exception as e:
        with open("Trackback.log","a") as fp:
            fp.write(f"{'#'*5}\t\tError Track Back Of {TIMEN}\t\t{'#'*5}\n\n")
            traceback.print_tb(e.__traceback__, limit=1000, file=fp)
            fp.write(f"\n {'#'*5}\t\t TLDR  error name [ {e} ] \t\t{'#'*5}\n\n")
        set_flag()

def main():
    files_ready()
    global TotalModules
    logging.info(f"Connection Stablishing.....")
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((HOST, PORT))
    client_socket.settimeout(SOCKET_TIMEOUT)
    logging.info(rf"Connection Stablished Sucessfully. ( {HOST}:{PORT} )")

    login_data = MESSAGE.login(USER_ID, PASSWORD, BROKER_ID, TRADER_ID)
    client_socket.send(login_data)
    response = client_socket.recv(1024)
    SESSION = Response(response)
    if SESSION.header.TransactionCode == 2301 and (
        not (SESSION.error or SESSION.checksum)
    ):
        logging.info("SucessFull Login")
        print("Login SucessFull")
    else:
        logging.info("Login Failed")
        print("\tLogin Failed. TryAgain")
        if EXIT_ON_LOGIN_FAIL:
            exit()

    SESSION.header.repack()
    TotalModules = SESSION.header.AlphaChar[0]
    for module in range(1, int(SESSION.header.AlphaChar[0])+1):
        print("Making New Drop Copy requests for module :-",module)
        drop_copy = MESSAGE.dropcopy(SESSION.header, module)
        client_socket.send(drop_copy)
    
    th = Thread(target=linear,args = (client_socket,SESSION))
    th.start()
    try:
        while  BOOM:
            display()
            time.sleep(0.2)
    except KeyboardInterrupt as k:
        set_flag()
    th.join()
    
    print("\n\nWriting the STACK data to file before exiting ")
    write_drop_copy("", on_exit=True)
    print("Exiting...")


if __name__ == "__main__":
    main()
