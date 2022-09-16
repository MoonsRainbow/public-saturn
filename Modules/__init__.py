import sys
import json
import typing
import pymysql
import datetime


def get_current_time():
    """
    현재 시간 가져오기 메소드
    :return: 현재 날짜시간, datetime 형식
    """
    return datetime.datetime.now()


def record_local_log(_log_file, _err, remark=None):
    """
    로컬 로그 작성 메소드
    :param _log_file: 로그 파일 경로
    :param _err: sys.exc_info()
    :param remark: 추가 내용
    """
    _log_str = _err
    _line_no = 0
    
    try:
        _line_no = _log_str[2].tb_lineno
    except AttributeError:
        _line_no = _log_str[2]
    
    finally:
        with open(_log_file, "a") as lf:
            lf.write("ERROR OCCURRED TIME   :: {}".format(datetime.datetime.now()) + "\n")
            lf.write("ERROR OCCURRED CLASS  :: {}".format(_log_str[0]) + "\n")
            lf.write("ERROR OCCURRED DESC   :: {}".format(_log_str[1]) + "\n")
            lf.write("ERROR OCCURRED LINE   :: {}".format(_line_no) + "\n")
            if remark is not None:
                lf.write("ERROR OCCURRED REMARK :: {}".format(remark) + "\n")
            lf.write("====================================================================================================" + "\n")


class _DataBaseManager:
    conn: pymysql.connect
    query_house: str
    connect_info: dict
    
    def __init__(self, _info: dict, _log_file):
        """
        데이터 베이스 매니저 생성자
        :param _info: 데이터 베이스 설정 정보
        :param _log_file: 로그 파일 경로
        """
        self.connect_info = _info
        self.log_file = _log_file
        self.connecting()
    
    def connecting(self):
        """
        데이터 베이스 연결 메소드
        """
        try:
            self.conn = pymysql.connect(
                host=self.connect_info["HOST"],
                port=self.connect_info["PORT"],
                user=self.connect_info["USER"],
                password=self.connect_info["PASSWORD"],
                database=self.connect_info["DATABASE"]
            )
        except:
            record_local_log(self.log_file, sys.exc_info(), self.connect_info)
    
    def disconnecting(self):
        """
        데이터 베이스 연결 종료 메소드
        """
        self.conn.close()
    
    @staticmethod
    def load_query(query_house, query_title):
        if not query_title.endswith(".sql"):
            query_title += ".sql"
        
        with open(query_house + query_title, "r") as sql_text:
            _sql = "".join(sql_text.readlines())
        return _sql
    
    def execute_query(self, sql):
        """
        쿼리 실행 메소드
        :param sql: 쿼리
        :return: INSERT 문일 경우=LAST_INSERT_ID // 그 외의 경우=쿼리 결과 값
        """
        _sql = sql.replace("    ", "").replace("        ", "").replace("            ", "").replace("\n", " ")
        _retry_count = 1
        _execute_status = True
        
        while _execute_status:
            _execute_result = True
            _response = None
            try:
                with self.conn.cursor() as _conn:
                    _conn.execute(_sql)
                    self.conn.commit()
                    
                    if _sql.lower().strip().startswith("insert", 0):
                        _response = _conn.lastrowid
                    else:
                        _response = _conn.fetchall()
            except (pymysql.err.ProgrammingError, pymysql.err.DataError):
                # ProgrammingError = 잘못된 SQL 쿼리문
                # DataError = 잘못된 데이터 형식 핸들링
                record_local_log(self.log_file, sys.exc_info(), remark=sql)
            except:
                # OperationalError = 데이터 베이스 접근 불가, 같이 처리
                _execute_result = False
                
                if _retry_count < 10:
                    self.conn.ping(reconnect=True)
                    _retry_count += 1
                else:
                    record_local_log(self.log_file, sys.exc_info(), remark=sql)
                    _execute_status = False
            finally:
                if _execute_result:
                    _execute_status = False
                    return _response


class _CoreModule:
    VER = 4.0
    AUTH = "MoonsRainbow"
    PATH: typing.Dict[str, any] = {"AbsRoot": "F:\\Saturn"}
    
    DATE_MARK = None
    TIME_MARK = None
    
    def __init__(self, _class_code, _identify_code):
        """
        모듈 코어 생성자
        :param _class_code: 모듈 분류 코드
        :param _identify_code: 모듈 인식 코드
        """
        self.CLASS_CODE = _class_code
        self.IDENTIFY_CODE = _identify_code
        
        self.DATE_MARK = self.set_date_mark()
        self.TIME_MARK = self.set_time_mark()
        
        self.PATH.__setitem__("DirConfig", self.PATH['AbsRoot'] + "\\Config")
        self.PATH.__setitem__("DirLocalLog", self.PATH['AbsRoot'] + "\\LocalLog")
        self.PATH.__setitem__("DirResource", self.PATH['AbsRoot'] + "\\Resources")
        
        self.LOG_FILE = self.set_log_file(self.PATH['DirLocalLog'], self.DATE_MARK, self.CLASS_CODE, self.IDENTIFY_CODE)
        
        self.DBM = self.set_dbm(self.PATH['DirConfig'], self.LOG_FILE)
        
    def logging(self, _err, _remark=None):
        """
        모듈 로그 작성 메소드
        :param _err: sys.exec_info()
        :param _remark: 추가 내용
        :return:
        """
        record_local_log(self.LOG_FILE, _err, _remark)
    
    @staticmethod
    def set_dbm(_root, _log_file):
        """
        dbm 생성 메소드
        :param _root: 설정 폴더 경로
        :param _log_file: 로그 파일 경로
        :return: DataBaseManager Class Instance
        """
        with open("{}\\database.json".format(_root), "r") as db_config:
            _dbf = json.load(db_config)
            _dbm = _DataBaseManager(
                _info={
                    "HOST": _dbf['HOST'],
                    "PORT": _dbf['PORT'],
                    "USER": _dbf['USER'],
                    "PASSWORD": _dbf['PASSWORD'],
                    "DATABASE": "saturn",
                },
                _log_file=_log_file
            )
        
        return _dbm
    
    @staticmethod
    def set_log_file(_root, _date_mark, _class_code, _identify_code):
        """
        로컬 로그 파일 경로 생성 메소드
        :param _root: 로컬 로그 폴더 경로
        :param _date_mark: 모듈 실행 날짜
        :param _class_code: 모듈 분류 코드
        :param _identify_code: 모듈 인식 코드
        :return: 로컬 로그 파일 경로
        """
        return "{}\\{}-{}-{}.txt".format(_root, _date_mark, _class_code, _identify_code)
    
    @staticmethod
    def set_date_mark():
        """
        고정 날짜 생성 메소드
        :return: str 타입 날짜, %Y%m%d 형식
        """
        return datetime.datetime.strftime(get_current_time(), "%Y%m%d")
    
    @staticmethod
    def set_time_mark():
        """
        고정 시간 생성 메소드
        :return: str 타입 시간, %H%M%S 형식
        """
        return datetime.datetime.strftime(get_current_time(), "%H%M%S")
