import os
import time
import socket
import struct
import hashlib
import logging
import datetime
import traceback
from config import *
from threading import Thread, Lock


CSV_FILE_HEADER = "ResponseOrderNumber,BrokerNumber,TraderNum,AccountNum,BuySell,OriginalVol,DisclosedVol,RemainingVol,DisclosedVolRemaining,Price,ST_ORDER_FLAGS,Gtd,FillNumber,FillQty,FillPrice,VolFilledToday,ActivityType,ActivityTime,OpOrderNumber,OpBrokerNumber,Symbol,Series,BookType,NewVolume,ProClient,PAN,AlgoID,LastActivityReference,NnfField\n"
TIMEN = time.strftime("%Y%m%d")
FILE_LOCK = Lock()
EXIT_FLAG = False
TOTAL_RECORDS = 0
RECORD_TIME = {1: datetime.datetime(1980, 1, 1, 0, 0, 0)}
STACK = []
CORRUPTION = False
PROGRAM_TIMER = time.time()
IS_TIMEOUT = False

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


logging.info("Trading Program Started")


def strftimesec(x) -> str:
    gps_epoch = datetime.datetime(1980, 1, 1, 0, 0, 0)
    utc_datetime = gps_epoch + datetime.timedelta(seconds=x)
    return utc_datetime

def write_drop_copy(save_csv="", on_exit=False):
    global STACK, PROGRAM_TIMER, TOTAL_RECORDS
    STACK.append(save_csv)
    TOTAL_RECORDS += 1
    if (
        len(STACK) >= STACK_SIZE
        or on_exit
        or (STACK_TIMER <= (time.time() - PROGRAM_TIMER))
    ):
        with open(
            f"TRADE_{SEGMENT_NAME}_DC_{TIMEN}.csv",
            "a",
        ) as fl:
            if TOTAL_RECORDS > 1 and not on_exit:
                fl.write("\n")
            fl.write("\n".join([i for i in STACK if i]))
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
        self.string = f"{self.instrument_name.decode().strip()}|{self.symbol.decode().strip()}|{self.expiry_date}|{self.strike_price}|{self.option_type.decode().strip()}|{self.CALevel}"


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
    def dropcopy(session_header, AlphaChar,IS_FIRST, module) -> bytes:
        header = MESSAGE(8000, 0, AlphaChar, session_header.TraderId, 0, 0, 0, 0, 48)
        if not RESUME_DOWNLOAD:
            c = 0
        else:
            c = track_me(None, module=0)
            # c = int.from_bytes(c, "big")
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
    def __init__(self, response_bytes: bytes):
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
        (
            self.DC0000_res_ord_num,
            self.DC0001_brk_num,
            self.__reserve,
            self.DC0002_trader_num,
            self.DC0003_account_num,
            self.DC0004_buy_sell,  #
            self.DC0005_orginal_vol,
            self.DC0006_disclosed_vol,
            self.DC0007_remaining_vol,
            self.DC0008_remaining_discls_vol,
            self.DC0090_price,
            self.DC0091_ST_ORDER_FLAGS,
            self.DC0092_gtd,
            self.DC0093_fill_num,
            self.DC0094_fill_qty,
            self.DC0095_fill_price,
            self.DC0096_vol_filled_today,
            self.DC0097_acticity_typ,
            self.DC0098_acticity_time,
            self.DC0099_op_ord_number,
            self.DC0991_op_brk_id,
            self.__reserved_1,
            self.DC0992_token,
            self.DC0993_contract_section,
            self.DC0994_open_close,
            self.DC0995_old_open_close,
            self.DC0996_book_type,
            self.__reserved_1,
            self.DC0997_new_vol,
            self.DC0998_old_account_num,
            self.DC0999_participant,
            self.DC0999_participant_old,
            self.DC1000_ADD_ORDER_FLAGS,
            self.__reserved_1,
            self.DC1001_give_up_trade,
            self.__reserved_1,
            self.DC1002_pan,
            self.DC1003_old_pan,
            self.DC1004_algo_id,
            self.DC1005_algo_category,
            self.DC1006_lst_acitvity_refrence,
            self.DC1007_Nnf_field,
            self.__reseve44,
        ) = struct.unpack(
            "!d5scl10shlllll2slllll2sld5scl28sccccl10s12s12s1sccc10s10slhqd44s",
            response_bytes,
        )
        # -----------------------------v---------v------this two offset are not mentioned in the documentetion
        # !d5scl10shlllll2slllll2sld5s[c]l28sccc[c]l10s12s12s1sccc10s10slhqd44s
        # they are added manually and thrown to reserved or negelected bytes
        self.clean()
        self.save_csv = " , ".join(
            [str(self.__getattribute__(i)) for i in sorted(self.__dir__()) if "DC" in i]
        )
        FILE_LOCK.acquire()
        write_drop_copy(save_csv=self.save_csv)
        FILE_LOCK.release()
        if self.DC0098_acticity_time > RECORD_TIME.get(
            th_id, datetime.datetime(1980, 1, 1, 0, 0, 0)
        ):
            RECORD_TIME[th_id] = self.DC0098_acticity_time

    def clean(self):
        self.DC0000_res_ord_num = int(self.DC0000_res_ord_num)
        self.DC0001_brk_num = self.DC0001_brk_num
        self.DC0002_trader_num = self.DC0002_trader_num
        self.DC0003_account_num = self.DC0003_account_num.decode("utf-8").strip()
        self.DC0004_buy_sell = self.DC0004_buy_sell
        self.DC0005_orginal_vol = self.DC0005_orginal_vol
        self.DC0006_disclosed_vol = self.DC0006_disclosed_vol
        self.DC0007_remaining_vol = self.DC0007_remaining_vol
        self.DC0008_remaining_discls_vol = self.DC0008_remaining_discls_vol
        self.DC0090_price = self.DC0090_price
        self.DC0091_ST_ORDER_FLAGS = StOdrFlg(self.DC0091_ST_ORDER_FLAGS).string
        self.DC0092_gtd = self.DC0092_gtd
        self.DC0093_fill_num = self.DC0093_fill_num
        self.DC0094_fill_qty = self.DC0094_fill_qty
        self.DC0095_fill_price = self.DC0095_fill_price
        self.DC0096_vol_filled_today = self.DC0096_vol_filled_today
        self.DC0097_acticity_typ = self.DC0097_acticity_typ.decode("utf-8").strip()
        self.DC0098_acticity_time = strftimesec(self.DC0098_acticity_time)
        self.DC0099_op_ord_number = int(self.DC0099_op_ord_number)
        self.DC0991_op_brk_id = self.DC0991_op_brk_id.decode("utf-8").strip()
        self.DC0992_token = self.DC0992_token
        self.DC0993_contract_section = ContractDesc(self.DC0993_contract_section).string
        self.DC0994_open_close = self.DC0994_open_close.decode("utf-8").strip()
        self.DC0995_old_open_close = self.DC0995_old_open_close.decode("utf-8").strip()
        self.DC0996_book_type = self.DC0996_book_type.decode("utf-8").strip()
        self.DC0997_new_vol = self.DC0997_new_vol
        self.DC0998_old_account_num = self.DC0998_old_account_num.decode(
            "utf-8"
        ).strip()
        self.DC0999_participant = self.DC0999_participant.decode("utf-8").strip()
        self.DC0999_participant_old = self.DC0999_participant_old.decode(
            "utf-8"
        ).strip()
        self.DC1000_ADD_ORDER_FLAGS = AddOrdFlg(self.DC1000_ADD_ORDER_FLAGS).string
        self.DC1001_give_up_trade = self.DC1001_give_up_trade.decode("utf-8").strip()
        self.DC1002_pan = self.DC1002_pan.decode("utf-8").strip()
        self.DC1003_old_pan = self.DC1003_old_pan.decode("utf-8").strip()
        self.DC1004_algo_id = self.DC1004_algo_id
        self.DC1005_algo_category = self.DC1005_algo_category
        self.DC1006_lst_acitvity_refrence = self.DC1006_lst_acitvity_refrence
        self.DC1007_Nnf_field = int(self.DC1007_Nnf_field)


def files_ready():
    global TOTAL_RECORDS
    logging.info("Making necessary File adjustments...")
    drop_file = f"TRADE_{SEGMENT_NAME}_DC_{TIMEN}.csv"
    if not os.path.exists(drop_file):
        with open(drop_file, "w") as fl:
            fl.write(CSV_FILE_HEADER)
    else:
        with open(drop_file, "r") as fl:
            TOTAL_RECORDS = len(fl.readlines()) - 1
    logging.info("File adjustments Completed.")


def suspend():
    period = 0
    while (not EXIT_FLAG) and (period <= 5):
        time.sleep(1)
        period += 1

def track_me(timestamp: bytes = None, module=0):
    if timestamp:
        with open(f"tracker_[{module}].csv", "wb") as fl:
            fl.write(timestamp)
    else:
        with open(f"tracker_[{module}].csv", "rb") as fl:
            return fl.read()



def thread_func(client_socket, SESSION, module: int):
    global CORRUPTION,IS_TIMEOUT
    if not os.path.exists(f"tracker_[{module}].csv"):
        with open(f"tracker_[{module}].csv", "wb") as fl:
            fl.write(b"\x00")

    IS_FIRST = True
    start_time = time.time()
    last_rec_time = None
    response = b""
    while not EXIT_FLAG:
        IS_FIRST = False

        elapsed_time = time.time() - start_time
        if elapsed_time > 29:
            print("SENDING HEART_BEAT.......", end="\n")
            logging.info("Sending HeartBeat...")
            heart_beat = MESSAGE.hearbeat(SESSION.header.TraderId)
            client_socket.send(heart_beat)
            start_time = time.time()
        try:
            response = b""
            logging.info("Logging singel bytes temporarly")
            response = client_socket.recv(318)
            logging.info(f"The recived data is {response}")
            IS_TIMEOUT = False
        except socket.timeout:
            IS_TIMEOUT = True
            logging.info("SocketTimeOut {response}")
            continue
        if not response:
            print(
                f"Server Didnot_respond ________ Suspending For A While. Module:-{module}",
                end="\n",
            )
            heart_beat = MESSAGE.hearbeat(SESSION.header.TraderId)
            client_socket.send(heart_beat)
            logging.info(
                f"Server Didnot_respond ________ Suspending For A While. Module:-{module}"
            )
            suspend()
            continue
        SESSION = Response(response)

        if SESSION.error or SESSION.checksum:
            CORRUPTION = True
            print("The Data In response is corrupted")
            print("Retrying..")
            continue
        elif SESSION.header.TransactionCode == 2222:
            last_rec_time = SESSION.header.TimeStamp1
            DC = DropCopy(SESSION.body, module)

        else:
            print(
                "TransactionCode",
                SESSION.header.TransactionCode,
                " [+] ",
                TransactionCode.dic.get(SESSION.header.TransactionCode, "Unknown"),
            )
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
            thread_func(client_socket, SESSION,0)
            BOOM = True
    
    except Exception as e:
        with open("TrackBack.log","a") as fp:
            traceback.print_tb(e.__traceback__, limit=100, file=fp)
        set_flag()

def main():
    files_ready()
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
    Total_Modules = (
        1 if b"\x00" in SESSION.header.AlphaChar else int(SESSION.header.AlphaChar)
    )
    for module in range(1, int(SESSION.header.AlphaChar[0])+1):
        print("Making New Drop Copy requests for module :-",module)
        drop_copy = MESSAGE.dropcopy(SESSION.header, module, 0, module)
        client_socket.send(drop_copy)
    
    th = Thread(target=linear,args = (client_socket,SESSION))
    th.start()
    try:
        while True:
            display()
            time.sleep(0.2)
    except KeyboardInterrupt as k:
        set_flag()
    th.join()
    
    print("\n\nWriting the STACK data to file before exiting ")
    write_drop_copy("", on_exit=True)
    print("Writing all the timestamps for analysis")
    print("Exiting...")


if __name__ == "__main__":
    main()
