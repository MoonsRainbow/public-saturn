import io
import time
import os
import copy
import shutil
import requests
from PIL import Image
from math import ceil
import multiprocessing
from multiprocessing import Pipe
from Modules import (sys, datetime, _CoreModule, record_local_log)

CLASS_CODE = "SATELLITE"
IDENTIFY_CODE = "CB"

# 코어 모듈 생성
CB = _CoreModule(CLASS_CODE, IDENTIFY_CODE)


def launch(_requester: Pipe, _receiver: Pipe):
    # 멀티프로세스 시작점 프리즈
    multiprocessing.freeze_support()
    try:
        # 디렉토리 절대 경로 선언
        CB.PATH['DirTemp'] = CB.PATH['AbsRoot'] + "\\Temp"
        CB.WATER_MARK_PATH = CB.PATH['DirResource'] + "\\water_mark.png"
        
        # API 주소 선언
        CB.SERVER = ""
        CB.API_URL = {}
        
        # 멀티 프로세스 갯수 컨트롤러
        CB.AIR_CRAFT_COUNT = 50
        
        # 멀티 프로세스 객체 저장소
        CB.AIR_CRAFT = {}
        
        # 중복된 필터 차량 저장소
        CB.CASH = {}

        # 작업 시작 신호 전송
        send_message(CB, _requester, True, "Do Work.")
        if not receive_message(CB, _receiver):
            raise SaturnSendReturnSignal()
        
        # 카베이 기준, 이전 만료 날짜 가져오기
        dead_line = CB.DBM.execute_query("SELECT last_date FROM home_sat_date_set WHERE satellite_id_code = \"{}\" AND flag_use = 1;".format(CB.IDENTIFY_CODE))[0][0]
        
        # 신규 만료 날짜 가져오기
        temp_line = datetime.date.today() - datetime.timedelta(days=28)
        
        # 이전 만료 날짜 유효성 체크
        if temp_line > dead_line:
            
            # 만료 날짜 업데이트
            CB.DBM.execute_query("UPDATE home_sat_date_set SET flag_use = 0 WHERE date_set_index != 0;")
            CB.DBM.execute_query("INSERT INTO home_sat_date_set(satellite_id_code, last_date, flag_use) VALUES(\"{}\", \"{}\", 1);".format(CB.IDENTIFY_CODE, temp_line))
            dead_line = temp_line

        # 데이터 싱크 & Filtering 구간
        # 작업 배열 생성
        work_list = []
        
        # 전체 작업 댓수 가져오기
        _saturn_total_vehicle_count = int(CB.DBM.execute_query("SELECT count(*) FROM saturn.saturn_vehicle_storage;")[0][0]) + 1
        
        # 멀티 프로세스 작업 배정
        for air_craft_index in range(CB.AIR_CRAFT_COUNT):
            temp_list = []
            
            # 1부터 전체 작업 댓수까지, 멀티 프로세스 갯수만큼 +
            for work_index in range(1 + air_craft_index, _saturn_total_vehicle_count, CB.AIR_CRAFT_COUNT):
                temp_list.append(work_index)
            work_list.append(copy.deepcopy(temp_list))

        # 프로세스 선언, 실행
        for _, _workload in enumerate(work_list):
            
            # 메세지 송수신 파이프 생성 (프로세스 간 통신용)
            _satellite_res, _air_craft_req = Pipe()
            _air_craft_res, _satellite_req = Pipe()
            
            # 프로세스 선언
            air_craft_process = multiprocessing.Process(
                target=launch_sync_filter_worker,
                args=(_,
                      _air_craft_req,
                      _air_craft_res,
                      CB.PATH['DirConfig'],
                      "{}-SF-WORKER-{}.txt".format(CB.LOG_FILE.split(".")[0], _),
                      dead_line,
                      _workload)
            )

            # 하위 프로세스 정보 저장
            CB.AIR_CRAFT.__setitem__(air_craft_process, {
                "RES": _satellite_res,
                "REQ": _satellite_req,
                "WORKLOAD": _workload
            })
            
            # 프로세스 시작
            air_craft_process.start()

        # 대시 보드용 데이터 객체
        # ERROR: 에러 발생 코드
        # DEAD: 데드 라인 바깥 차량
        # NON CATE: 번역되지 않거나, 사용하지 않는 차량
        # DONE: Air Craft 작업 완료
        # NEW: 새로 등록된 차량
        # UPDATE: 정보 업데이트 차량
        # DELETE: 삭제 처리 차량
        _sync_filter_result = {
            "TOTAL": _saturn_total_vehicle_count,
            "ERROR": 0,
            "DEAD": 0,
            "NON CATE": 0,
            "NEW": 0,
            "UPDATE": 0,
            "DELETE": 0,
            "UNDEFINED": 0,
            "PERCENT": 0
        }

        # 작동 중인 프로세스 갯수가 0이 될 때까지 무한 루프
        while len(CB.AIR_CRAFT) > 0:
            _remark = None
            
            # 완료 처리된 프로세스 임시 저장소
            _is_done = []
            
            for _air_craft, _tools in CB.AIR_CRAFT.items():
                
                # 처리 중인 프로세스 정보 저장
                _remark = _air_craft
                try:
                    
                    # 프로세스 생존 여부 확인
                    if _air_craft.is_alive():
                        
                        # 메세지 수신
                        _result = _tools['RES'].recv()

                        # 메세지 수신 후 결과 코드에 따라 대시 보드 데이터 업데이트
                        if _result['RESULT']['CODE'] == "ERROR":
                            _sync_filter_result["ERROR"] += 1

                        elif _result['RESULT']['CODE'] == "DEAD":
                            _sync_filter_result['DEAD'] += 1

                        elif _result['RESULT']['CODE'] == "NON CATE":
                            _sync_filter_result['NON CATE'] += 1

                        elif _result['RESULT']['CODE'] == "DONE":
                            if _remark not in _is_done:
                                _is_done.append(_remark)

                        else:
                            # 현재 처리 불가한 차량 캐시에 저장 처리
                            if "CASH" in _result['RESULT'].keys():
                                for _k, _v in _result['RESULT']['CASH'].items():
                                    CB.CASH.__setitem__(_k, _v)

                            if _result['RESULT']['CODE'] == "NEW":
                                _sync_filter_result['NEW'] += 1

                            elif _result['RESULT']['CODE'] == "UPDATE":
                                _sync_filter_result['UPDATE'] += 1

                            elif _result['RESULT']['CODE'] == "DELETE":
                                _sync_filter_result['DELETE'] += 1

                            else:
                                _sync_filter_result['UNDEFINED'] += 1
                    
                    # 프로세스가 죽은 상태면
                    else:
                        # 완료 처리된 프로세스 임시 저장소에 저장
                        if _remark not in _is_done:
                            _is_done.append(_remark)
                
                # 프로세스 처리 중 에러 발생 시
                except:
                    # 완료 처리된 프로세스 임시 저장소에 저장
                    if _remark not in _is_done:
                        _is_done.append(_remark)

            # 보고용 퍼센트 계산
            _sync_filter_result["PERCENT"] = round(
                ((
                     _sync_filter_result["ERROR"]
                     + _sync_filter_result["DEAD"]
                     + _sync_filter_result["NON CATE"]
                     + _sync_filter_result["NEW"]
                     + _sync_filter_result["UPDATE"]
                     + _sync_filter_result["DELETE"]
                     + _sync_filter_result["UNDEFINED"]
                 ) / _sync_filter_result["TOTAL"]) * 100, 2
            )
            
            # Saturn 에 현재 상황 메세지 발신
            send_message(CB, _requester, True, "Data is {}% synchronized. Detail: T:{}=DD:{}+DU:{}+NW:{}+UP:{}+DE:{}+UN:{}+ER:{}"
                         .format(_sync_filter_result["PERCENT"],
                                 _sync_filter_result["TOTAL"],
                                 _sync_filter_result["DEAD"],
                                 _sync_filter_result["NON CATE"],
                                 _sync_filter_result["NEW"],
                                 _sync_filter_result["UPDATE"],
                                 _sync_filter_result["DELETE"],
                                 _sync_filter_result["UNDEFINED"],
                                 _sync_filter_result["ERROR"]))
            
            # 응답 받을 때까지 강제 대기
            receive_message(CB, _receiver, True)

            # 완료 처리된 프로세스가 있을 경우
            if len(_is_done) > 0:
                
                # 프로세스 객체 배열에서 삭제
                for _air_craft in _is_done:
                    CB.AIR_CRAFT.__delitem__(_air_craft)

            # 하위 프로세스들에게 응답 일괄 발신
            for _air_craft, _tools in CB.AIR_CRAFT.items():
                try:
                    _tools['REQ'].send(True)
                except:
                    continue

        # 무한 루프 종료 후 좀비 프로세스 방지용 프로세스 킬
        for _air_craft in CB.AIR_CRAFT.keys():
            _air_craft.join()
            
        # 데이터 랭킹 구간
        # 필터 정보에 입력된 필터 인덱스 번호, 최대 업로드 댓수, 프리미어 마크 부착 댓수 가져오기
        _cb_filter_list = CB.DBM.execute_query("""
        SELECT filter_set_index, max_allow, premier_max_rank
        FROM saturn.home_cb_filter_set FILTER
        WHERE FILTER.flag_use = 1
        ORDER BY FILTER.`rank`;""")

        # 대시 보드 객체 초기화
        _rank_result = {
            "TOTAL": len(_cb_filter_list),
            "COMPLETE": 0,
            "PERCENT": 0
        }

        # 필터 정보로 FOR 문 시작
        for _cb_filter_info in _cb_filter_list:
            
            # 캐시 데이터 처리 배열 생성
            _used_cash_core = []
            
            # 정보 데이터 변수 선언
            _filter_index = int(_cb_filter_info[0])
            _max_allow = int(_cb_filter_info[1]) + 1

            # 캐시 데이터 처리 구문
            for _cash_core, _cash_filter_index in CB.CASH.items():
                
                # 캐시된 차량이 해당 필터에 속할 경우
                if _filter_index in _cash_filter_index:
                    
                    # 사용된 차량 처리
                    _used_cash_core.append(_cash_core)
                    
                    # 차량 필터 인덱스 업데이트
                    CB.DBM.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET filter_set_index = {} WHERE vehicle_core_index = {}".format(_filter_index, _cash_core))
                    
            # 가격 오름차순으로 카베이 등록 차량 정보 가져오기
            # 카베이 코어 인덱스 번호, 상태, 가격
            _vehicle_list = CB.DBM.execute_query("""
            SELECT CB_CORE.vehicle_core_index, CB_STS.value, CB_CORE.`rank`
            FROM saturn.sat_sup_cb_vehicle_core CB_CORE,
                 saturn.sat_sup_cb_vehicle_status CB_STS,
                 saturn.sat_sup_cb_vehicle_price CB_PRC
            WHERE CB_CORE.vehicle_core_index = CB_STS.vehicle_core_index
              AND CB_CORE.vehicle_core_index = CB_PRC.vehicle_core_index
              AND CB_STS.flag_use = 1
              AND CB_PRC.flag_use = 1
              AND CB_CORE.filter_set_index = {}
            ORDER BY CB_PRC.value;""".format(_filter_index))

            # 랭크 초기화
            _rank = 1
            
            # 랭크 시작
            for _vehicle in _vehicle_list:
                
                # 데이터 변수 초기화
                _cb_core_index = _vehicle[0]
                _cb_status = _vehicle[1]
                _cb_rank = _vehicle[2]

                # _cb_rank 가 -2 보다 클 경우
                # _cb_rank = -2: 사망하거나, 번역이 안되어 있는 차량
                if _cb_rank > -2:
                    
                    # 상태가 판매 중인 경우
                    if _cb_status == 0:
                        
                        # 랭크 값이 업로드 가능 댓수 보다 작을 경우
                        if _rank < _max_allow:
                            
                            # 랭크 값 초기화
                            CB.DBM.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET `rank` = {} WHERE vehicle_core_index = {};".format(_rank, _cb_core_index))
                            _rank += 1
                            
            # 위 FOR 문이 끝나고 난 후 차량 랭크 분류
            # _cb_rank = -2: 사망하거나, 번역이 안되어 있는 차량
            # _cb_rank = -1: 순위권 밖 차량
            # _cb_rank >  0: 판매 중이 되는 차량

            # 캐시 차량 유효성 검사
            for _cash_core in _used_cash_core:
                
                # 캐시 차량의 랭크 값이 0보다 큰지 확인
                _cash_core_rank = int(CB.DBM.execute_query("SELECT `rank` FROM saturn.sat_sup_cb_vehicle_core CB_CORE WHERE CB_CORE.vehicle_core_index = {};".format(_cash_core))[0][0])

                # 캐시 차량의 랭크 값이 0보다 큰 경우
                if _cash_core_rank > 0:
                    
                    # 캐시에서 해당 차량 삭제
                    CB.CASH.__delitem__(_cash_core)
                else:
                    # 다른 필터에서 순위권에 들 수 있으므로 다시 캐시에 추가
                    CB.DBM.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET filter_set_index = {} WHERE vehicle_core_index = {}".format(-99, _cash_core))

            # 보고서 수치 계산
            _rank_result["COMPLETE"] += 1
            _rank_result["PERCENT"] = round((_rank_result["COMPLETE"] / _rank_result["TOTAL"]) * 100, 2)
            
            # 보고서 메세지 발신
            send_message(CB, _requester, True, "Data is {}% ranked. Detail: T:{}=C:{}, F-Index={}".format(_rank_result["PERCENT"], _rank_result["TOTAL"], _rank_result["COMPLETE"], _filter_index))
            
            # Saturn 응답 대기
            receive_message(CB, _receiver, True)
        
        # 랭크 구간이 끝나고 캐시 차량 삭제 처리
        # 삭제 하지 않으면 그대로 판매 중으로 노출될 가능성이 있음
        _duplicate_filter_list = CB.DBM.execute_query("""
        SELECT CORE.vehicle_core_index, CORE.unique_index
        FROM sat_sup_cb_vehicle_core CORE
             LEFT JOIN sat_sup_cb_vehicle_status STS ON CORE.vehicle_core_index = STS.vehicle_core_index
        WHERE STS.flag_use = 1 AND filter_set_index = -99;
        """)
        for _duplicate_vehicle in _duplicate_filter_list:
            _cb_core_index = _duplicate_vehicle[0]
            _cb_unique_index = _duplicate_vehicle[1]
            CB.DBM.execute_query("CALL CB_RecordVehicleStatus({}, {});".format(_cb_core_index, 4))
            requests.request("PUT", CB.API_URL["CHANGE_STATUS"], data={"flag": 4, "itemNo": _cb_unique_index})
            time.sleep(0.3)
        
        # 데이터 관리 구간
        # 랭킹 구간에서 가져온 필터 목록을 그대로 사용
        _filter_count = 1
        for _cb_filter_info in _cb_filter_list:
            _cb_filter_index = _cb_filter_info[0]
            _cb_premier_rank = _cb_filter_info[2]

            # 카베이 차량 정보 목록 가져오기
            # 차대번호 인덱스, 차대번호, 카베이 인덱스 번호, 카테고리 인덱스, CB 번호, 가격, 커미션, 상태, 랭크
            _cb_vehicle_list = CB.DBM.execute_query("""
            SELECT CB_CORE.storage_index,
                   STOR.chassis_no,
                   CB_CORE.vehicle_core_index,
                   CB_CORE.class_index,
                   CB_CORE.unique_index,
                   CB_PRC.value,
                   CB_PRC.sub_value,
                   CB_STS.value,
                   CB_CORE.`rank`
            FROM saturn.sat_sup_cb_vehicle_core CB_CORE,
                 saturn.sat_sup_cb_vehicle_status CB_STS,
                 saturn.sat_sup_cb_vehicle_price CB_PRC,
                 saturn.saturn_vehicle_storage STOR
            WHERE CB_CORE.vehicle_core_index = CB_STS.vehicle_core_index
              AND CB_CORE.vehicle_core_index = CB_PRC.vehicle_core_index
              AND CB_CORE.storage_index = STOR.storage_index
              AND CB_STS.flag_use = 1
              AND CB_PRC.flag_use = 1
              AND filter_set_index = {}
            ORDER BY CB_CORE.`rank`;
            """.format(_cb_filter_index))

            # 활동 보고서 데이터 초기화
            _activity_result = {
                "FILTER INDEX": _cb_filter_index,
                "TOTAL": len(_cb_vehicle_list),
                "DELETE": 0,
                "DEAD": 0,
                "UPLOAD": 0,
                "UPDATE": 0,
                "NO RANK": 0,
                "DUPLICATE": 0,
                "FAIL": 0,
                "ERROR": 0,
                "PERCENT": 0
            }

            _remark = None
            _cb_premier_count = 1
            # 처리 시작
            for _cb_vehicle_info in _cb_vehicle_list:

                # 정보 데이터 변수 초기화
                _storage_index = int(_cb_vehicle_info[0])
                _chassis_no = _cb_vehicle_info[1]
                _cb_core_index = int(_cb_vehicle_info[2])
                _cb_class_index = int(_cb_vehicle_info[3])
                _cb_unique_index = int(_cb_vehicle_info[4])
                _cb_price = int(_cb_vehicle_info[5])
                _cb_comm = int(_cb_vehicle_info[6])
                _cb_status = int(_cb_vehicle_info[7])
                _cb_rank = int(_cb_vehicle_info[8])

                _remark = _cb_core_index
                try:
                    # 랭크가 0보다 클 경우
                    if _cb_rank > 0:
                        
                        # 판매 중 차량일 경우
                        if _cb_status == 0:
                            
                            # 신규 차량일 경우
                            if _cb_unique_index == 0:
                                
                                # 등록 가능한 차대번호인지 체크
                                _check_chassis_result = requests.request("GET", CB.API_URL["CHECK_CHASSIS"] + _chassis_no).text
                                
                                # 등록 가능한 차량일 경우
                                if _check_chassis_result == "true":
                                    # 번역 정보 받아오기
                                    _vehicle = list(CB.DBM.execute_query("CALL HOME_TransrateData({}, {}, {})".format(_storage_index, _cb_core_index, _cb_class_index))[0])
                                    _cb_class_type = int(_vehicle[24])
                                    _flag_none = False
                                    _item_count = 0
                                    
                                    # 모든 정보 유효한지 체크
                                    for _ in _vehicle:
                                        if _cb_class_type == 1 and _item_count > 24:
                                            continue
                                        else:
                                            if _ is None:
                                                if _item_count == 18:
                                                    # 알 수 없는 색상 -> 기타 처리
                                                    _vehicle[_item_count] = 13
                                                else:
                                                    _flag_none = True
                                        _item_count += 1
                                        
                                    # 데이터가 유효하지 않을 경우
                                    if _flag_none:
                                        
                                        # 보고서 객체 업데이트
                                        _activity_result["FAIL"] += 1
                                        
                                    # 데이터가 유효할 경우
                                    else:
                                        # 프리미어 뱃지 탈/부착 처리
                                        if _cb_premier_count <= _cb_premier_rank:
                                            _cb_premier_count += 1
                                            _cb_premier_flag = 1
                                        else:
                                            _cb_premier_flag = 0

                                        # 데이터 객체 초기화
                                        _vehicle_main_data = {
                                            "PRICE": _cb_price,
                                            "COMMISSION": _cb_comm,
                                            "DISCOUNT": 0,
                                            "CHS NO": _vehicle[0],
                                            "FORM YEAR": _vehicle[1],
                                            "MAKER": _vehicle[2],
                                            "MODEL": _vehicle[3],
                                            "DETAIL MODEL": _vehicle[4],
                                            "VEHICLE TYPE ID": _vehicle[5],
                                            "MILEAGE": _vehicle[6],
                                            "DISPLACEMENT": _vehicle[7],
                                            "SEAT": _vehicle[8],
                                            "TRANSMISSION ID": _vehicle[9],
                                            "FUEL ID": _vehicle[10],
                                            "DOOR": _vehicle[11],
                                            "DRIVE TYPE ID": _vehicle[12],
                                            "HANDLE ID": 0,
                                            "INITIAL YEAR": datetime.datetime.strftime(_vehicle[13], '%Y-%m-%d'),
                                            "WEIGHT": _vehicle[14],
                                            "LENGTH": _vehicle[15],
                                            "HEIGHT": _vehicle[16],
                                            "WIDTH": _vehicle[17],
                                            "COLOR ID": _vehicle[18],
                                            "CFVT NO": _vehicle[19],
                                            "LINE CBM": float(_vehicle[20]),

                                            "KOR MAKER": _vehicle[25],
                                            "KOR MODEL": _vehicle[26],
                                            "FORM ID": _vehicle[27],
                                            "DETAIL FORM ID": _vehicle[28],
                                            "CAPACITY ID": _vehicle[29]
                                        }

                                        # 기타 정보 초기화
                                        _vehicle_sub_data = {
                                            "OPTION OUTSIDE": [],
                                            "OPTION INSIDE": [],
                                            "OPTION SAFE": [],
                                            "UNDER STATUS": [
                                                {
                                                    "underNo": 1,
                                                    "flag": 1
                                                },
                                                {
                                                    "underNo": 2,
                                                    "flag": 0
                                                },
                                                {
                                                    "underNo": 3,
                                                    "flag": 0
                                                }
                                            ],
                                            "WORKING STATUS": [
                                                {
                                                    "workingNo": 1,
                                                    "flag": 1
                                                },
                                                {
                                                    "workingNo": 2,
                                                    "flag": 1
                                                },
                                                {
                                                    "workingNo": 3,
                                                    "flag": 1
                                                },
                                                {
                                                    "workingNo": 4,
                                                    "flag": 1
                                                },
                                                {
                                                    "workingNo": 5,
                                                    "flag": 1
                                                },
                                                {
                                                    "workingNo": 6,
                                                    "flag": 1
                                                },
                                                {
                                                    "workingNo": 7,
                                                    "flag": 1
                                                }
                                            ]
                                        }

                                        # 기타 정보 - 작동 상태 - 4륜 구동 여부 데이터 초기화
                                        if _vehicle_main_data.get("DRIVE TYPE ID") == 1:
                                            _vehicle_sub_data.get("WORKING STATUS").append({"workingNo": 8, "flag": 2})
                                        else:
                                            _vehicle_sub_data.get("WORKING STATUS").append({"workingNo": 8, "flag": 1})
                                            
                                        # 아래 강제 옵션 이외의 옵션은 다미엔 과장의 요청으로 사용하지 않음

                                        # 에어컨 옵션 강제 추가 구문
                                        for __ in _vehicle_sub_data.get("OPTION INSIDE"):
                                            if __.get("optionNo") == 1:
                                                break
                                        else:
                                            _vehicle_sub_data.get("OPTION INSIDE").append({"optionNo": 1, "flag": 1})

                                        # 파워 스티어링 옵션 강제 추가 구문
                                        for __ in _vehicle_sub_data.get("OPTION INSIDE"):
                                            if __.get("optionNo") == 8:
                                                break
                                        else:
                                            _vehicle_sub_data.get("OPTION INSIDE").append({"optionNo": 8, "flag": 1})

                                        # 파워윈도우 옵션 강제 추가 구문
                                        for __ in _vehicle_sub_data.get("OPTION INSIDE"):
                                            if __.get("optionNo") == 21:
                                                break
                                        else:
                                            _vehicle_sub_data.get("OPTION INSIDE").append({"optionNo": 21, "flag": 1})

                                        # 운전자 에어백 옵션 강제 추가 구문
                                        for __ in _vehicle_sub_data.get("OPTION SAFE"):
                                            if __.get("optionNo") == 1:
                                                break
                                        else:
                                            _vehicle_sub_data.get("OPTION SAFE").append({"optionNo": 1, "flag": 1})

                                        # ABS 옵션 강제 추가 구문
                                        for __ in _vehicle_sub_data.get("OPTION SAFE"):
                                            if __.get("optionNo") == 10:
                                                break
                                        else:
                                            _vehicle_sub_data.get("OPTION SAFE").append({"optionNo": 10, "flag": 1})

                                        # 일반 차량의 경우
                                        if _cb_class_type == 1:
                                            payload = {
                                                "itemInfo": {
                                                    "sellPrice": _vehicle_main_data.get("PRICE"),
                                                    "discountPrice": _vehicle_main_data.get("DISCOUNT"),
                                                    "commission": _vehicle_main_data.get("COMMISSION"),
                                                    "chassisNo": _vehicle_main_data.get("CHS NO"),
                                                    "year": _vehicle_main_data.get("FORM YEAR"),
                                                    "maker": _vehicle_main_data.get("MAKER"),
                                                    "model": _vehicle_main_data.get("MODEL"),
                                                    "detailModel": _vehicle_main_data.get("DETAIL MODEL"),
                                                    "grade": "PAGO ESPECIAL",
                                                    "product": _vehicle_main_data.get("VEHICLE TYPE ID"),
                                                    "mileage": _vehicle_main_data.get("MILEAGE"),
                                                    "cc": _vehicle_main_data.get("DISPLACEMENT"),
                                                    "seats": _vehicle_main_data.get("SEAT"),
                                                    "transmission": _vehicle_main_data.get("TRANSMISSION ID"),
                                                    "fuel": _vehicle_main_data.get("FUEL ID"),
                                                    "door": _vehicle_main_data.get("DOOR"),
                                                    "drive": _vehicle_main_data.get("DRIVE TYPE ID"),
                                                    "handle": 0,
                                                    "initialDate": _vehicle_main_data.get("INITIAL YEAR"),
                                                    "weight": _vehicle_main_data.get("WEIGHT"),
                                                    "slength": _vehicle_main_data.get("LENGTH"),
                                                    "width": _vehicle_main_data.get("WIDTH"),
                                                    "height": _vehicle_main_data.get("HEIGHT"),
                                                    "color": _vehicle_main_data.get("COLOR ID"),
                                                    "memo": None,
                                                    "premier": _cb_premier_flag,
                                                    "cfvtNo": _vehicle_main_data.get("CFVT NO"),
                                                    "lineCbm": _vehicle_main_data.get("LINE CBM")
                                                    # SUV (대중소형)
                                                },
                                                "itemOptionOutside": _vehicle_sub_data.get("OPTION OUTSIDE"),
                                                "itemOptionInside": _vehicle_sub_data.get("OPTION INSIDE"),
                                                "itemOptionSafe": _vehicle_sub_data.get("OPTION SAFE"),
                                                "itemUnderStatus": _vehicle_sub_data.get("UNDER STATUS"),
                                                "itemWorkingStatus": _vehicle_sub_data.get("WORKING STATUS")
                                            }
                                            # 일반 자동차 등록
                                            _upload_result = requests.post(CB.API_URL["UPLOAD_CAR"], json=payload).json()
                                        
                                        # 트럭 차량의 경우
                                        else:
                                            payload = {
                                                "itemInfo": {
                                                    "sellPrice": _vehicle_main_data.get("PRICE"),
                                                    "discountPrice": _vehicle_main_data.get("DISCOUNT"),
                                                    "commission": _vehicle_main_data.get("COMMISSION"),
                                                    "chassisNo": _vehicle_main_data.get("CHS NO"),
                                                    "year": _vehicle_main_data.get("FORM YEAR"),
                                                    "maker": _vehicle_main_data.get("KOR MAKER"),
                                                    "model": _vehicle_main_data.get("KOR MODEL"),
                                                    "engMaker": _vehicle_main_data.get("MAKER"),
                                                    "engModel": _vehicle_main_data.get("MODEL"),
                                                    "form": _vehicle_main_data.get("FORM ID"),
                                                    "formDetail": _vehicle_main_data.get("DETAIL FORM ID"),
                                                    "capacity": _vehicle_main_data.get("CAPACITY ID"),
                                                    "grade": "PAGO ESPECIAL",
                                                    "mileage": _vehicle_main_data.get("MILEAGE"),
                                                    "cc": _vehicle_main_data.get("DISPLACEMENT"),
                                                    "transmission": _vehicle_main_data.get("TRANSMISSION ID"),
                                                    "fuel": _vehicle_main_data.get("FUEL ID"),
                                                    "weight": _vehicle_main_data.get("WEIGHT"),
                                                    "slength": _vehicle_main_data.get("LENGTH"),
                                                    "width": _vehicle_main_data.get("WIDTH"),
                                                    "height": _vehicle_main_data.get("HEIGHT"),
                                                    "color": _vehicle_main_data.get("COLOR ID"),
                                                    "memo": "* CC, 톤수, 무게, 길이, 너비, 높이는 세턴에서 판단하지 못합니다. 반드시 꼭 확인해주세요.",
                                                    "premier": _cb_premier_flag,
                                                    "cfvtNo": _vehicle_main_data.get("CFVT NO"),
                                                    "lineCbm": _vehicle_main_data.get("LINE CBM")
                                                }
                                            }
                                            # 일반 자동차 등록
                                            _upload_result = requests.post(CB.API_URL["UPLOAD_TRUCK"], json=payload).json()

                                        # 업로드 결과 변수 초기화
                                        _cb_new_unique_index = _upload_result.get("itemNo")
                                        _cb_old_unique_index = _upload_result.get("carNo")

                                        # 업로드 성공인지 확인
                                        if _cb_new_unique_index is None or _cb_old_unique_index is None:
                                            _activity_result["ERROR"] += 1
                                            continue

                                        # 생성된 CB, CN 번호 저장
                                        CB.DBM.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET unique_index = {}, sub_unique_index = {} WHERE vehicle_core_index = {};".format(_cb_new_unique_index, _cb_old_unique_index, _cb_core_index))

                                        # 사진 갯수, 카매니저 고유 번호 변수 초기화
                                        _photo_count = _vehicle[21]
                                        _cm_unique_index = _vehicle[23]

                                        _photo_list = []
                                        # 사진 갯수 만큼 FOR 문
                                        for _photo_index in range(1, _photo_count + 1):
                                            try:
                                                # 카매니저 CDN 주소 초기화
                                                _photo_url = "".format(_cm_unique_index, _photo_index)
                                                
                                                # 카매니저 CDN Request
                                                image_response = requests.request("GET", url=_photo_url, data={}, stream=True)

                                                # 사진 데이터 다운로드 (바이너리로 디스크에 저장)
                                                with open("{0}\\{1}.png".format(CB.PATH["DirTemp"], _photo_index), 'wb') as photo_file:
                                                    shutil.copyfileobj(image_response.raw, photo_file)
                                                    
                                                # Response 데이터 삭제 처리
                                                del image_response
                                            except:
                                                CB.logging(sys.exc_info(), "Cannot download photo, {}".format(_remark))
                                            else:
                                                # 사진 목록에 이름 저장
                                                _photo_list.append("{0}\\{1}.png".format(CB.PATH["DirTemp"], _photo_index))

                                        try:
                                            # 사진 객체 초기화
                                            files = {}
                                            
                                            # 워터마크 이미지 파일 오픈
                                            water_mark = Image.open(CB.WATER_MARK_PATH)
                                            
                                            # 이미지 리사이징 처리 시작
                                            for img_no in range(0, len(_photo_list)):
                                                
                                                # 원본 경로 선언
                                                origin_img_path = _photo_list[img_no]
                                                
                                                # 원본 이미지 바이너리 객체로 오픈
                                                bin_origin_image = open(origin_img_path, "rb")
                                                
                                                # 워터마크 이미지 바이너리 객체로 초기화
                                                bin_water_mark_image = io.BytesIO()
                                                
                                                # 미니 이미지 바이너리 객체로 초기화
                                                bin_mini_image = io.BytesIO()
                                                
                                                # 빅 이미지 바이너리 객체로 초기화
                                                bin_big_image = io.BytesIO()

                                                # 편집을 위한 이미지 오픈
                                                origin_image = Image.open(origin_img_path)

                                                # 워터마크 이미지 리사이징
                                                water_mark_image = origin_image.resize((1024, 768))
                                                water_mark_image_width = water_mark_image.size[0]
                                                water_mark_image_height = water_mark_image.size[1]

                                                # 워터마크 위치 계산
                                                water_mark_x = int(water_mark_image_width / 2) - 250
                                                water_mark_y = int(water_mark_image_height / 2) - 48

                                                # 워터마크 붙이기
                                                water_mark_image.paste(water_mark, (water_mark_x, water_mark_y), mask=water_mark)
                                                
                                                # 워터마크 이미지 저장
                                                water_mark_image.save(bin_water_mark_image, "PNG", quality=90)

                                                # 미니 이미지 리사이징
                                                mini_image = origin_image.resize((320, 240))
                                                
                                                # 미니 이미지 저장
                                                mini_image.save(bin_mini_image, "PNG", quality=50)

                                                # 빅 이미지 리사이징
                                                big_image = origin_image.resize((1024, 768))
                                                
                                                # 빅 이미지 저장
                                                big_image.save(bin_big_image, "PNG", quality=90)

                                                # 사진 객체에 저장
                                                files.__setitem__("file{}".format(img_no + 1), bin_origin_image)
                                                files.__setitem__("mini{}".format(img_no + 1), bin_mini_image.getvalue())
                                                files.__setitem__("big{}".format(img_no + 1), bin_big_image.getvalue())
                                                files.__setitem__("watermark{}".format(img_no + 1), bin_water_mark_image.getvalue())

                                            # 사진 업로드
                                            requests.request("POST", CB.API_URL["UPLOAD_IMAGE"], data={"itemNo": _cb_new_unique_index, "carNo": _cb_old_unique_index, "category": _cb_class_type}, files=files)
                                        finally:
                                            # 메모리에 저장된 사진 객체 삭제
                                            del files

                                        # 이미지 임시 저장 폴더 비우기
                                        for _temp in os.listdir(CB.PATH["DirTemp"]):
                                            try:
                                                os.remove("{}\\{}".format(CB.PATH["DirTemp"], _temp))
                                            except:
                                                pass

                                        # 보고서 객체 업데이트
                                        _activity_result["UPLOAD"] += 1
                                        
                                # 차대번호가 등록 불가능한 차량일 경우
                                else:
                                    # 제외 처리
                                    CB.DBM.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET `rank` = -3 WHERE vehicle_core_index = {}".format(_cb_core_index))
                                    
                                    # 보고서 객체 업데이트
                                    _activity_result["DUPLICATE"] += 1
                            
                            # 이미 업로드된 차량일 경우
                            else:
                                # 프리미어 뱃지 갯수 체크
                                if _cb_premier_count <= _cb_premier_rank:
                                    # 프리미어 뱃지 부착 가능
                                    _cb_premier_count += 1
                                    _cb_premier_flag = 1
                                else:
                                    # 프리미어 뱃지 부착 불가능
                                    _cb_premier_flag = 0

                                # 프리미어 뱃지 탈/부착
                                requests.request("PUT", CB.API_URL["CHANGE_PREMIER"], data={"premier": _cb_premier_flag, "itemNo": _cb_unique_index})
                                time.sleep(0.3)
                                
                                # 판매 중 처리
                                requests.request("PUT", CB.API_URL["CHANGE_STATUS"], data={"flag": 0, "itemNo": _cb_unique_index})
                                time.sleep(0.3)
                                
                                # 가격 업데이트
                                requests.request("PUT", CB.API_URL["CHANGE_PRICE"], data={"sellPrice": int(_cb_price), "discountPrice": 0, "commission": _cb_comm, "itemNo": _cb_unique_index})
                                time.sleep(0.3)

                                # 보고서 객체 업데이트
                                _activity_result["UPDATE"] += 1
                        
                        # 삭제된 차량 일 경우
                        else:
                            if _cb_unique_index != 0:
                                # 삭제 처리
                                requests.request("PUT", CB.API_URL["CHANGE_STATUS"], data={"flag": 4, "itemNo": _cb_unique_index})
                                time.sleep(0.3)
                                
                            # 보고서 객체 업데이트
                            _activity_result["DELETE"] += 1
                    
                    # 랭크가 0보다 작을 경우
                    else:
                        if _cb_unique_index != 0:
                            # 삭제 처리
                            requests.request("PUT", CB.API_URL["CHANGE_STATUS"], data={"flag": 4, "itemNo": _cb_unique_index})
                            time.sleep(0.3)

                        # 보고서 객체 업데이트
                        if _cb_rank <= -2:
                            _activity_result["DEAD"] += 1
                        else:
                            if _cb_status == 0:
                                _activity_result["NO RANK"] += 1
                            else:
                                _activity_result["DELETE"] += 1
                except TypeError:
                    CB.logging(sys.exc_info(), _remark)
                    _activity_result["FAIL"] += 1
                except:
                    CB.logging(sys.exc_info(), _remark)
                    _activity_result["ERROR"] += 1
                finally:
                    # 보고서 수치 계산
                    _activity_result["PERCENT"] = round(
                        ((
                            _activity_result["DELETE"]
                            + _activity_result["DEAD"]
                            + _activity_result["UPLOAD"]
                            + _activity_result["UPDATE"]
                            + _activity_result["NO RANK"]
                            + _activity_result["DUPLICATE"]
                            + _activity_result["FAIL"]
                            + _activity_result["ERROR"]
                         ) / _activity_result["TOTAL"]) * 100, 2)
                    
                    # Saturn 에 활동 보고서 제출
                    send_message(CB, _requester, True, "Filter Index {} ({}/{}) is {}% act. Detail: T:{}=UPL:{}+UPD:{}+DEL:{}+NRK:{}+DUP:{}+F:{}+DD:{}+ERR:{}".format(
                        _activity_result["FILTER INDEX"],
                        _filter_count,
                        len(_cb_filter_list),
                        _activity_result["PERCENT"],
                        _activity_result["TOTAL"],
                        _activity_result["UPLOAD"],
                        _activity_result["UPDATE"],
                        _activity_result["DELETE"],
                        _activity_result["NO RANK"],
                        _activity_result["DUPLICATE"],
                        _activity_result["FAIL"],
                        _activity_result["DEAD"],
                        _activity_result["ERROR"]
                    ))
                    
                    # 응답 대기
                    receive_message(CB, _receiver, True)
                    
            # 보고서를 활동 테이블에 저장
            CB.DBM.execute_query("""
            INSERT INTO saturn.sat_sup_cb_activity(filter_set_index, total, uploaded, updated, deleted, no_rank, duplicated, failed, dead, `error`)
            VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {})""".format(
                _activity_result["FILTER INDEX"],
                _activity_result["TOTAL"],
                _activity_result["UPLOAD"],
                _activity_result["UPDATE"],
                _activity_result["DELETE"],
                _activity_result["NO RANK"],
                _activity_result["DUPLICATE"],
                _activity_result["FAIL"],
                _activity_result["DEAD"],
                _activity_result["ERROR"]
            ))
            _filter_count += 1
        
    except SatelliteCannotControl:
        CB.logging(sys.exc_info(), "Satellite is out of control.")
    except SaturnSendReturnSignal:
        CB.logging(sys.exc_info(), "Saturn send return signal.")
    except:
        CB.logging(sys.exc_info(), "Satellite is broken.")
        send_message(CB, _requester, False, "Broken.")
        receive_message(CB, _receiver)
    finally:
        send_message(CB, _requester, False, "Landing.")
        receive_message(CB, _receiver)


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


def launch_sync_filter_worker(_air_craft_no, _requester, _receiver, _config_root, _ac_log_file,  _dead_line, _workload):
    """
    싱크 필터 처리 프로세스
    :param _air_craft_no: 프로세스 번호
    :param _requester: 메세지 발신용 파이프 객체
    :param _receiver: 메세지 수신용 파이프 객체
    :param _config_root: 설정 파일 디렉토리 절대 경로
    :param _ac_log_file: 프로세스 로그 파일
    :param _dead_line: 데이터 만료 날짜
    :param _workload: 작업량
    """
    
    # 필요 라이브러리 선언, 전역 변수 선언
    import typing
    _air_craft_dbm = None
    _remark = None
    try:
        # 데이터 베이스 객체 생성
        _air_craft_dbm = _CoreModule.set_dbm(_config_root, _ac_log_file)
        
        # 작업 루프 시작
        for _work_index in _workload:
            
            # 현재 작업 인덱스 저장
            _remark = _work_index
            
            # 작업 결과 보고서 객체 선언
            _work_report: typing.Dict[str, any] = {
                "RESULT": {
                    "CODE": "SUCCESS",
                    "CASH": {}
                }
            }
            
            # 처리 시작
            try:
                # 카매니저 차량 정보 가져오기
                # 연식, 사진 갯수, 업데이트 날짜, 판매중/삭제 플래그, 가격(원)
                _temp_cm_info = _air_craft_dbm.execute_query("""
                SELECT CM_CORE.vehicle_core_index,
                       CM_CORE.year,
                       CM_CORE.photo_count,
                       CM_CORE.update_date,
                       IF(CM_STS.value = "N" OR CM_STS.value = "U", 0, 4),
                       CM_PRC.value * 10000
                FROM saturn.sat_res_cm_vehicle_core CM_CORE,
                     saturn.sat_res_cm_vehicle_status CM_STS,
                     saturn.sat_res_cm_vehicle_price CM_PRC
                WHERE CM_CORE.vehicle_core_index = CM_STS.vehicle_core_index
                  AND CM_CORE.vehicle_core_index = CM_PRC.vehicle_core_index
                  AND CM_STS.flag_use = 1 AND CM_PRC.flag_use = 1
                  AND storage_index = {};""".format(_work_index))
                
                # 카매니저 차량 정보 존재 여부 확인
                if len(_temp_cm_info) > 0:
                    
                    # 데이터 변수 초기화
                    _cm_core_index = int(_temp_cm_info[0][0])
                    _cm_year = int(_temp_cm_info[0][1])
                    _cm_photo_count = int(_temp_cm_info[0][2])
                    _cm_update_date = _temp_cm_info[0][3]
                    _cb_status = int(_temp_cm_info[0][4])
                    _cm_price = int(_temp_cm_info[0][5])
                    
                    # 번역 정보 가져오기
                    # 카베이 카테고리 번호(없으면 0), 카베이 코어 테이블 인덱스 번호(없으면 0), CB 번호(없으면 0)
                    _temp_cb_info = _air_craft_dbm.execute_query("""
                    SELECT IF(count(*) = 1, T_CLASS.cb_class_index, 0),
                           IF(CB_CORE.vehicle_core_index is null, 0, CB_CORE.vehicle_core_index),
                           IF(CB_CORE.unique_index is null, 0, CB_CORE.unique_index)
                    FROM saturn.sat_res_cm_vehicle_core CM_CORE
                             JOIN sat_res_cm_category_class CM_CLASS ON CM_CORE.class_code = CM_CLASS.class_code
                             JOIN home_transrate_class T_CLASS ON CM_CLASS.class_index = T_CLASS.cm_class_index
                             LEFT JOIN sat_sup_cb_vehicle_core CB_CORE ON CM_CORE.storage_index = CB_CORE.storage_index
                    WHERE CM_CLASS.flag_use = 1
                      AND CM_CORE.storage_index = {};
                    """.format(_work_index))[0]
                    
                    # 번역 데이터 변수 초기화
                    _t_cb_class_index = int(_temp_cb_info[0])
                    _cb_core_index = int(_temp_cb_info[1])
                    _cb_unique_index = int(_temp_cb_info[2])
                    
                    # 카매니저 업데이트 날짜가 데드 라인 안쪽
                    if _cm_update_date >= _dead_line:
                        
                        # 카베이에 번역되어 있고, 사용 중인 차량
                        if _t_cb_class_index != 0:
                            
                            # 카베이 광고가 계산
                            if _cm_price <= 2990000:
                                _krw_fees = 500000
                            elif _cm_price <= 4990000:
                                _krw_fees = 400000
                            elif _cm_price <= 9990000:
                                _krw_fees = 300000
                            else:
                                _krw_fees = 200000
                                
                            # DB 고정 환율 로직
                            # _krw_rate = int(_air_craft_dbm.execute_query("""
                            # SELECT krw_rate
                            # FROM saturn.home_cb_price_set
                            # WHERE flag_use = 1
                            # ORDER BY record_date DESC;""")[0][0])
                            
                            # 2021-10-07
                            # MoonsRainbow
                            # 다미엔 과장, 가격 범위 별 환율 별도 설정 추가
                            if _cm_price <= 5000000:
                                _krw_rate = 1130
                            elif _cm_price <= 10000000:
                                _krw_rate = 1150
                            else:
                                _krw_rate = 1160
                            
                            # 가격 계산 공식 적용
                            _cb_price = ceil((_cm_price + _krw_fees) / _krw_rate)
                            
                            # 가격 정보 변수 초기화
                            _temp_price = _cb_price
                            _temp_commission = 0
                            _cb_commission = 0
                            
                            # 2021-12-06 다미엔 과장, 카베이 커미션 삭제 요청
                            # if _temp_price <= 2999:
                            #     _temp_commission = 250
                            # elif _temp_price <= 4999:
                            #     _temp_commission = 300
                            # elif _temp_price <= 9999:
                            #     _temp_commission = 350
                            # elif _temp_price <= 19999:
                            #     _temp_commission = 450
                            # else:
                            #     _temp_commission = 550
                            #
                            # if (_temp_price + _temp_commission) <= 2999:
                            #     _cb_commission = 250
                            # elif (_temp_price + _temp_commission) <= 4999:
                            #     _cb_commission = 300
                            # elif (_temp_price + _temp_commission) <= 9999:
                            #     _cb_commission = 350
                            # elif (_temp_price + _temp_commission) <= 19999:
                            #     _cb_commission = 450
                            # else:
                            #     _cb_commission = 550
                            
                            # 커미션 계산
                            # 현재는 커미션이 어차피 0원이라 무의미
                            _cb_price = _temp_price + _cb_commission
                            
                            # 세턴 업로드 필터 인덱스 가져오기
                            # 필터 인덱스 번호
                            _cb_filter_index_list = _air_craft_dbm.execute_query("""
                            SELECT filter_set_index
                            FROM saturn.home_cb_filter_set FILTER
                            WHERE class_index = {0}
                              AND flag_use = 1
                              AND (min_year <= {1}
                                AND {1} <= max_year)
                              AND (min_usd_price <= {2}
                                AND {2} <= max_usd_price)
                              AND min_photo_count <= {3};
                            """.format(_t_cb_class_index, _cm_year, _cb_price, _cm_photo_count))
                            
                            # 해당하는 필터 인덱스 없음
                            if len(_cb_filter_index_list) < 1:
                                
                                # 차량 정보에 필터 인덱스 초기화
                                _cb_filter_index = -1
                                
                            # 해당하는 필터 인덱스가 여러개
                            elif len(_cb_filter_index_list) > 1:
                                # 차량 정보에 캐시 처리중 값 부여
                                _cb_filter_index = -99
                                
                                # 이미 등록된 차량일 경우, 캐시 저장 처리
                                if _cb_core_index != 0:
                                    _cash = []
                                    for _temp_filter_index_list in _cb_filter_index_list:
                                        _cash.append(_temp_filter_index_list[0])

                                    # 보고서 객체 업데이트
                                    _work_report['RESULT']['CASH'] = {
                                        _cb_core_index: _cash
                                    }
                                    
                            # 해당하는 필터 인덱스가 1개
                            else:
                                
                                # 차량 정보에 필터 인덱스 고정
                                _cb_filter_index = int(_cb_filter_index_list[0][0])
                                
                            # 등록 안된 차량일 경우
                            if _cb_core_index == 0:
                                
                                # 차량 신규 등록
                                _cb_core_index = _air_craft_dbm.execute_query("CALL CB_StoreVehicle({}, {}, {});".format(_work_index, _cb_filter_index, _t_cb_class_index))[0][0]

                                # 보고서 객체 업데이트
                                _work_report['RESULT']['CODE'] = "NEW"
                                
                                # 해당하는 필터 인덱스가 여러개
                                if len(_cb_filter_index_list) > 1:
                                    
                                    # 캐시 저장 처리 재실행
                                    _cash = []
                                    for _temp_filter_index_list in _cb_filter_index_list:
                                        _cash.append(_temp_filter_index_list[0])
                                        
                                    # 보고서 객체 업데이트
                                    _work_report['RESULT']['CASH'] = {
                                        _cb_core_index: _cash
                                    }
                                    
                            # 이미 등록된 차량일 경우
                            else:
                                # 검증된 필터 인덱스로 초기화
                                _air_craft_dbm.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET filter_set_index = {} WHERE vehicle_core_index = {}".format(_cb_filter_index, _cb_core_index))
                                
                                # 필터 인덱스 초기화 후 삭제 시키지 못하고 쓰레기 데이터로 남는 차량 삭제 처리
                                if _cb_filter_index == -1:
                                    if _cb_unique_index != 0:
                                        requests.request("PUT", "", data={"flag": 4, "itemNo": _cb_unique_index})
                                        time.sleep(0.2)

                                # 보고서 객체 업데이트
                                if _cb_status == 0:
                                    _work_report['RESULT']['CODE'] = "UPDATE"
                                else:
                                    _work_report['RESULT']['CODE'] = "DELETE"
                                
                            # 이미 등록된 차량 정보 업데이트
                            if _cb_core_index != 0:
                                # 상태, 가격 업데이트
                                _air_craft_dbm.execute_query("CALL CB_RecordVehicleStatus({}, {});".format(_cb_core_index, _cb_status))
                                _air_craft_dbm.execute_query("CALL CB_RecordVehiclePrice({}, {}, {});".format(_cb_core_index, _cb_price, _cb_commission))
                                
                                # 랭크 초기화
                                _air_craft_dbm.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET `rank` = {} WHERE vehicle_core_index = {}".format(-1, _cb_core_index))
                        else:
                            # 카베이에 번역되어 있지 않은 차량
                            _work_report['RESULT']['CODE'] = "NON CATE"
                            if _cb_core_index != 0:
                                # 랭크 초기화
                                _air_craft_dbm.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET `rank` = {} WHERE vehicle_core_index = {}".format(-2, _cb_core_index))
                                
                                # 상태 업데이트 (삭제)
                                _air_craft_dbm.execute_query("CALL CB_RecordVehicleStatus({}, {});".format(_cb_core_index, 4))
                    else:
                        # 카매니저 업데이트 날짜가 데드라인 바깥쪽
                        _work_report['RESULT']['CODE'] = "DEAD"
                        if _cb_core_index != 0:
                            # 랭크 초기화
                            _air_craft_dbm.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET `rank` = {} WHERE vehicle_core_index = {}".format(-2, _cb_core_index))
                            
                            # 상태 업데이트 (삭제)
                            _air_craft_dbm.execute_query("CALL CB_RecordVehicleStatus({}, {});".format(_cb_core_index, 4))
                            
                            # 데이터 사망 처리
                            _air_craft_dbm.execute_query("UPDATE saturn.sat_sup_cb_vehicle_core SET filter_set_index = {} WHERE vehicle_core_index = {}".format(-1, _cb_core_index))

                        # 사망한 데이터 즉시 삭제 처리
                        if _cb_unique_index != 0:
                            requests.request("PUT", "", data={"flag": 4, "itemNo": _cb_unique_index})
                            time.sleep(0.2)
                else:
                    # 불량 데이터 처리
                    _work_report['RESULT']['CODE'] = "NON CATE"
            except:
                _work_report['RESULT']['CODE'] = "ERROR"
                record_local_log(_ac_log_file, sys.exc_info(), _remark)
            finally:
                # 보고서 제출
                _requester.send(_work_report)
                _receiver.recv()
    except:
        record_local_log(_ac_log_file, sys.exc_info(), _remark)
        _requester.send({
            "RESULT": {
                "CODE": "ERROR"
            }
        })
    else:
        _requester.send({
            "RESULT": {
                "CODE": "DONE",
                "DATA": None
            }
        })
    finally:
        try:
            _air_craft_dbm.disconnecting()
        except:
            pass
