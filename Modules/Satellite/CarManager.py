import os
import shutil
import ftplib
import datetime
from multiprocessing import Pipe
from Modules import (sys, json, _CoreModule)

CLASS_CODE = "SATELLITE"
IDENTIFY_CODE = "CM"

# 코어 모듈 생성
CM = _CoreModule(CLASS_CODE, IDENTIFY_CODE)


def launch(_requester: Pipe, _receiver: Pipe):
    try:
        # 디렉토리 절대 경로 선언
        CM.PATH.__setitem__("DirData", CM.PATH['AbsRoot'] + "\\DataFile")
        
        # 카매니저 FTP 인증 정보 가져오기
        with open(CM.PATH['DirConfig'] + "\\CarManagerFTP.json", "r") as ftp_config:
            _ftp = json.load(ftp_config)
            
            CM.FTP_AUTH = {
                "host": _ftp['host'],
                "user": _ftp['user'],
                "password": _ftp['password']
            }
        
        # 새로운 데이터 체크
        result, data = check_new_data(CM.FTP_AUTH)
        
        # 데이터 체크 성공 시
        if result:
            
            # 데이터 파일 존재 여부 재확인
            if len(data) > 0:
                # 새로운 데이터 파일이 있을 경우,
                # Saturn 에 작업 시작 메세지 전송
                send_message(CM, _requester, True, "Do Work.")
                
                # 응답 메세지 수신 대기
                if not receive_message(CM, _receiver):
                    # 응답 메세지 수신 중 에러 발생 처리
                    raise SaturnSendReturnSignal()
                
                # 새로운 데이터 파일 다운로드
                if download_new_data(CM.FTP_AUTH, data):
                    send_message(CM, _requester, True, "Download Done.")
                    if not receive_message(CM, _receiver):
                        raise SaturnSendReturnSignal()
                    
                    for _ in data:
                        # 데이터 파일 날짜 추출
                        _update_date = _.split("_")[1]
                        _update_date = datetime.date(int(_update_date[:4]), int(_update_date[4:6]), int(_update_date[6:]))
                        _data_file = "{}\\{}".format(CM.PATH['DirData'], _)
                        
                        # 데이터 파싱
                        _parsed_result, _parsed_data = parsing_new_data(_data_file)
                        
                        if _parsed_result["CODE"]:
                            # 파싱 결과 메세지 발신
                            send_message(CM, _requester, True, "File {} (T:{}=S:{}+F:{}) Parsing Done."
                                         .format(_, _parsed_result["TOTAL"], _parsed_result["SUCCESS"], _parsed_result["FAIL"]))
                            if not receive_message(CM, _receiver):
                                raise SaturnSendReturnSignal()
                            
                            # 데이터 저장
                            _store_result = store_new_data(_update_date, _parsed_data)
                            
                            # 데이터 저장 결과 메세지 발신
                            send_message(CM, _requester, True, "Data (T:{}=S:{}+F:{}) Store Done."
                                         .format(_store_result["TOTAL"], _store_result["SUCCESS"], _store_result["FAIL"]))
                            if not receive_message(CM, _receiver):
                                raise SaturnSendReturnSignal()
                        else:
                            send_message(CM, _requester, True, "File {} Parsing Failed.".format(_))
                            if not receive_message(CM, _receiver):
                                raise SaturnSendReturnSignal()
            else:
                # 새로운 데이터 파일이 없을 경우,
                # Saturn 에 작업 없음 메세지 전송
                send_message(CM, _requester, True, "Nothing Work.")
                if not receive_message(CM, _receiver):
                    raise SaturnSendReturnSignal()
        else:
            send_message(CM, _requester, False, data)
            if not receive_message(CM, _receiver):
                raise SaturnSendReturnSignal()
    except SatelliteCannotControl:
        CM.logging(sys.exc_info(), "Satellite is out of control.")
    except SaturnSendReturnSignal:
        CM.logging(sys.exc_info(), "Saturn send return signal.")
    except:
        CM.logging(sys.exc_info(), "Satellite is broken.")
        send_message(CM, _requester, False, "Broken.")
        receive_message(CM, _receiver)
    else:
        send_message(CM, _requester, False, "Landing.")
        receive_message(CM, _receiver)
    finally:
        pass


def send_message(_satellite, _requester: Pipe, _status, _msg: str):
    """
    메세지 발신 메소드
    :param _satellite: 위성 객체
    :param _requester: 발신용 파이프 객체
    :param _status: 현재 상태
    :param _msg: 메세지
    """
    try:
        # 메세지 발신
        _requester.send({
            "STATUS": _status,
            "SEND_TIME": "{}:{}".format(_satellite.set_date_mark(), _satellite.set_time_mark()),
            "MESSAGE": _msg
        })
    except:
        _satellite.logging(sys.exc_info(), "Can not send a message.")


def receive_message(_satellite, _receiver: Pipe, _force_wait=False):
    """
    메세지 수신 메소드
    :param _satellite: 위성 객체
    :param _receiver: 수신용 파이프 객체
    :param _force_wait: 강제 대기 플래그 (Default=False,대기 안함)
    :return: 수신 여부
    """
    response = True
    try:
        # 강제 대기일 경우
        if _force_wait:
            
            # 수신할 때까지 무한 대기
            response = _receiver.recv()
        
        # 강제 대기가 아닐 경우
        else:
            
            # 최대 10초 동안만 대기
            # 수신하면 즉시 처리
            response = _receiver.poll(10)
    except:
        response = False
        _satellite.logging(sys.exc_info(), "Can not receive a message.")
    finally:
        return response


class SatelliteCannotControl(Exception):
    def __init__(self):
        return


class SaturnSendReturnSignal(Exception):
    def __init__(self):
        return


def check_new_data(_ftp_auth: dict):
    """
    새로운 데이터 파일 여부 체크
    
    :param _ftp_auth: Dictionary-FTP 인증 정보
    :return: _result: Boolean-성공/실패 여부, _data: List[String]-새로운 데이터 파일명 목록
    """
    
    # 전역 변수 선언
    _result = True
    _data = []
    
    # FTP 객체 할당
    _ftp = ftplib.FTP()
    try:
        # FTP 연결, 로그인, 경로 지정
        _ftp.connect(_ftp_auth['host'])
        _ftp.login(_ftp_auth['user'], _ftp_auth['password'])
        _ftp.cwd('./send')
        
        # FTP 경로 내에 파일 목록 가져오기
        _file_list = _ftp.retrlines("list").split("\n")
        _ftp_file_list = []
        for _file_name in _file_list:
            if len(_file_name) > 0:
                if _file_name.split(" ")[-1] != "log":
                    _ftp_file_list.append(_file_name.split(" ")[-1])
        
        # Saturn 경로 내에 파일 목록 가져오기
        _local_file_list = os.listdir(CM.PATH['DirData'])
        
        # FTP 파일 목록, Saturn 파일 목록 비교
        while True:
            try:
                # 파일 명이 같이 경우, 두 목록에서 삭제
                if _ftp_file_list[0] == _local_file_list[0]:
                    _ftp_file_list.__delitem__(0)
                    _local_file_list.__delitem__(0)
                else:
                    _data = _ftp_file_list
                    break
            except IndexError:
                # 두 목록의 길이가 달라, IndexError 발생 시 새로운 파일로 인식
                _data = _ftp_file_list
                break
    
    except:
        _result = False
        _data = "Failed to Check FTP File."
        CM.logging(sys.exc_info(), "Failed to Check FTP File.")
    else:
        _result = True
    finally:
        try:
            _ftp.quit()
        except:
            pass
        
        return _result, _data


def download_new_data(_ftp_auth: dict, _data_list: list):
    """
    새로운 데이터 파일 다운로드
    
    :param _ftp_auth: Dictionary-FTP 인증 정보
    :param _data_list: List[String]-새로운 데이터 파일명 목록
    :return: _result: Boolean-성공/실패 여부
    """
    
    # 전역 변수 선언
    _result = True
    
    # FTP 객체 할당
    _ftp = ftplib.FTP()
    try:
        # FTP 연결, 로그인, 경로 지정
        _ftp.connect(_ftp_auth['host'])
        _ftp.login(_ftp_auth['user'], _ftp_auth['password'])
        _ftp.cwd('./send')
        
        for _data in _data_list:
            with open(_data, "wb") as _dp:
                # 데이터 파일 다운로드
                _ftp.retrbinary("RETR {}".format(_data), _dp.write)

            # 데이터 파일 디렉토리에 복사/붙여넣기
            shutil.copyfile(_data, "{}\\{}".format(CM.PATH['DirData'], _data))
            os.remove(_data)
    except:
        _result = False
        CM.logging(sys.exc_info(), "Failed to Download FTP File.")
    else:
        _result = True
    finally:
        try:
            _ftp.quit()
        except:
            pass
        
        return _result


def parsing_new_data(_file):
    """
    데이터 파싱
    * 해당 메소드는 카매니저에서 제공한 데이터 파일의 규칙을 확인해서 직접 만든 메소드임.
    * 따라서 부정확한 부분이 아직 남아 있음.
    
    :param _file: 데이터 파일 명
    :return: _result: Dictionary{
        CODE: Boolean-성공/실패 여부,
        TOTAL: Int-전체 갯수,
        SUCCESS: Int-성공 갯수,
        FAIL: Int-실패 갯수
    }, _parsed_data: List[Dictionary]-파싱 데이터 객체 목록
    """
    
    # 전역 변수 선언
    _result = {
        "CODE": True,
        "TOTAL": 0,
        "SUCCESS": 0,
        "FAIL": 0
    }
    _parsed_data = []
    try:
        with open(_file, "r") as _origin:
            _row_count = 0
            for _row in _origin.readlines():
                try:
                    if _row[0] == "H" or _row[0] == "T":
                        continue
                    else:
                        _result["TOTAL"] += 1
                        _row_count += 1
                        
                        _row_data = {}
                        _str_data = ""
                        _str_data_count = 0
                        
                        for _col in range(9, len(_row)):
                            if _row[_col] == " ":
                                try:
                                    if _str_data is not None:
                                        if _row[_col + 1] == " ":
                                            _str_data_count += 1
                                            _row_data.__setitem__(_str_data_count, _str_data)
                                            _str_data = None
                                            continue
                                        else:
                                            _str_data += _row[_col]
                                    else:
                                        continue
                                except IndexError:
                                    continue
                            else:
                                if _str_data is None:
                                    _str_data = ""
                                
                                _str_data += _row[_col]
                        
                        _row_data.__setitem__(2, _row_data.get(2)[:10])
                        if len(_row_data) == 20:
                            _row_data.__delitem__(3)
                            _row_data.__delitem__(4)
                            _row_data.__delitem__(5)
                        elif len(_row_data) == 21:
                            _row_data.__delitem__(3)
                            _row_data.__delitem__(4)
                            _row_data.__delitem__(5)
                            _row_data.__delitem__(6)
                        else:
                            _row_data.__delitem__(3)
                            _row_data.__delitem__(4)
                            _row_data.__delitem__(5)
                            _row_data.__delitem__(6)
                            _row_data.__delitem__(7)
                        
                        re_check = False
                        for k, v in _row_data.items():
                            if "다크그레이" in v:
                                re_check = True
                        
                        if re_check:
                            if _str_data_count == 20:
                                _row_data.__delitem__(6)
                            elif _str_data_count == 21:
                                _row_data.__delitem__(7)
                            else:
                                pass
                        
                        _temp_data = {}
                        _temp_index = 0
                        for k, v in _row_data.items():
                            _temp_data.__setitem__(_temp_index, v)
                            _temp_index += 1
                        _row_data = _temp_data
                        
                        temp_color = ""
                        if len(_row_data) == 16:
                            for k, v in _row_data.items():
                                if "다크그레이" in v:
                                    _row_data.__setitem__(k, str(_row_data.get(k)).replace("다크그레이", ""))
                                    temp_color = "다크그레이"
                        else:
                            temp_color = str(_row_data.get(8))
                            _row_data.__delitem__(8)
                        
                        _temp_data = {}
                        _temp_index = 0
                        for k, v in _row_data.items():
                            _temp_data.__setitem__(_temp_index, v)
                            _temp_index += 1
                        _row_data = _temp_data
                        
                        _remark = str(_row_data.get(0))[1:]
                        _data = {
                            "STATUS": str(_row_data.get(0))[0],
                            "UNIQUE_INDEX": str(_row_data.get(0))[1:],
                            "CLASS_CODE": str(_row_data.get(1)),
                            "CAR_NUMBER": str(_row_data.get(2)),
                            "TRANSMISSION_CODE": str(_row_data.get(3)),
                            "TRANSMISSION_VALUE": str(_row_data.get(4)),
                            "FUEL_CODE": str(_row_data.get(5)),
                            "FUEL_VALUE": str(_row_data.get(6)),
                            "COLOR_CODE": str(_row_data.get(7)),
                            "COLOR_VALUE": temp_color,
                            "MILEAGE": int(str(_row_data.get(8))[0:10]),
                            "YEAR": int(str(_row_data.get(8))[10:14]),
                            "INITIAL_DATE": str(_row_data.get(8))[14:22],
                            "PRICE": int(str(_row_data.get(8))[22:32]),
                            "OPTIONS": str(_row_data.get(8))[32:96],
                            "PHOTO_COUNT": int(str(_row_data.get(8))[96:99]),
                            "ASSOCIATION": str(_row_data.get(8))[99:],
                            "CITY": str(_row_data.get(9)),
                            "COMPLEX": str(_row_data.get(10)),
                            "COMPANY": str(_row_data.get(11)),
                            "DEALER": str(_row_data.get(12)),
                            "CONTACT": str(_row_data.get(13)),
                            "CHASSIS_NO": str(_row_data.get(14)),
                            "REGISTRATION_DATE": str(_row_data.get(15))
                        }
                        
                        _parsed_data.append(_data)
                        _result["SUCCESS"] += 1
                except:
                    _result["FAIL"] += 1
    except:
        _result['CODE'] = False
    finally:
        return _result, _parsed_data


def store_new_data(_update_date, _data_list):
    """
    
    :param _update_date: Date-소속 날짜
    :param _data_list: List[Dictionary]-파싱 데이터 객체 목록
    :return: _result: Dictionary{
        TOTAL: Int-전체 갯수,
        SUCCESS: Int-성공 갯수,
        FAIL: Int-실패 갯수
    }
    """
    
    # 전역 변수 선언
    _result = {
        "TOTAL": 0,
        "SUCCESS": 0,
        "FAIL": 0
    }
    for _vehicle in _data_list:
        try:
            _chassis_no = _vehicle.get("CHASSIS_NO")
            _storage_index = CM.DBM.execute_query("CALL SATURN_StoreVehicle(\"{}\");".format(_chassis_no))[0][0]
            
            _unique_index = _vehicle.get("UNIQUE_INDEX")
            _car_number = _vehicle.get("CAR_NUMBER")
            _class_code = _vehicle.get("CLASS_CODE")
            _transmission_code = _vehicle.get("TRANSMISSION_CODE")
            _fuel_code = _vehicle.get("FUEL_CODE")
            _color_code = _vehicle.get("COLOR_CODE")
            _mileage = _vehicle.get("MILEAGE")
            _year = _vehicle.get("YEAR")
            _photo_count = _vehicle.get("PHOTO_COUNT")
            _initial_date = _vehicle.get("INITIAL_DATE")
            _registration_date = _vehicle.get("REGISTRATION_DATE")
            
            # 저장 프로시저 호출
            _vehicle_core_index = CM.DBM.execute_query("CALL CM_StoreVehicle({}, {}, \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", {}, {}, {}, \"{}\", \"{}\", \"{}\")".format(
                _unique_index,
                _storage_index,
                _car_number,
                _class_code,
                _transmission_code,
                _fuel_code,
                _color_code,
                _mileage,
                _year,
                _photo_count,
                _initial_date,
                _registration_date,
                _update_date
            ))[0][0]
            
            # 상태 등록 프로시져 호출
            _status = _vehicle.get("STATUS")
            CM.DBM.execute_query("CALL CM_RecordVehicleStatus({}, \"{}\");".format(_vehicle_core_index, _status))
            
            # 가격 등록 프로시져 호출
            _price = _vehicle.get("PRICE")
            CM.DBM.execute_query("CALL CM_RecordVehiclePrice({}, {});".format(_vehicle_core_index, _price))
            
            # 신규 차량일 경우,
            if _status == "N":
                
                # 옵션 등록
                _options = _vehicle.get("OPTIONS")
                _option_index = 1
                for _option in _options:
                    CM.DBM.execute_query("INSERT INTO saturn.sat_res_cm_vehicle_option(vehicle_core_index, option_index, value) VALUES({}, {}, \"{}\")".format(
                        _vehicle_core_index,
                        _option_index,
                        _option))
                    _option_index += 1
                
                # 판매처 정보 등록
                _city = _vehicle.get("CITY")
                _association = _vehicle.get("ASSOCIATION")
                _complex = _vehicle.get("COMPLEX")
                _company = _vehicle.get("COMPANY")
                _dealer = _vehicle.get("DEALER")
                _contact = _vehicle.get("CONTACT")
                CM.DBM.execute_query("INSERT INTO saturn.sat_res_cm_vehicle_contact(vehicle_core_index, city, association, complex, company, dealer, contact) VALUES({}, \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\")".format(
                    _vehicle_core_index,
                    _city,
                    _association,
                    _complex,
                    _company,
                    _dealer,
                    _contact
                ))
        except:
            _result["FAIL"] += 1
            CM.logging(sys.exc_info(), "Cannot Store Data Row.")
        else:
            _result["SUCCESS"] += 1
        finally:
            _result["TOTAL"] += 1
    
    return _result
