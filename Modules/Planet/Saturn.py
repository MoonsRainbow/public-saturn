import time
import multiprocessing
from multiprocessing import Pipe
# Modules 라이브러리 디렉토리를 호출 시, __init__.py 파일이 자동 실행
from Modules import (sys, _CoreModule)
from Modules.Satellite import (CarManager, CarBayKorea)


class Saturn(_CoreModule):
    CLASS_CODE = "PLANET"
    IDENTIFY_CODE = "SATURN"
    
    # 자원 위성 카테고리
    RESOURCE_SATELLITE = [CarManager]
    
    # 공급 위성 카테고리
    SUPPLY_SATELLITE = [CarBayKorea]
    
    # 궤도 (작업 진행 중인 위성 객체)
    IN_ORBIT = {}
    
    # 발사대 (작업 대기 중인 위성 배열)
    IN_LAUNCHER = []
    
    # 세턴 모듈 생성자
    def __init__(self):
        # 코어 모듈 오버라이딩
        _CoreModule.__init__(self, self.CLASS_CODE, self.IDENTIFY_CODE)

        # 자원 위성 발사대에 장착
        for _ in self.RESOURCE_SATELLITE:
            self.IN_LAUNCHER.append(_.IDENTIFY_CODE)

        # 공급 위성 발사대에 장착
        for _ in self.SUPPLY_SATELLITE:
            self.IN_LAUNCHER.append(_.IDENTIFY_CODE)
        
    # 위성 발사 메소드
    def launch(self, satellite):
        # 메세지 송수신 파이프 생성 (프로세스 간 통신용)
        # 양방향 파이프 생성도 있으나, 정상 작동하지 않아 단방향 2개로 생성
        planet_res, satellite_req = Pipe()
        satellite_res, planet_req = Pipe()
    
        # 위성 프로세스 할당, 위성 송수신 파이프 전달
        satellite_process = multiprocessing.Process(
            target=satellite.launch,
            args=(satellite_req, satellite_res)
        )
    
        # 위성 프로세스 생성
        satellite_process.start()
        print("{} : Launched".format(satellite.IDENTIFY_CODE))
    
        # 궤도 배열에 프로세스 포인터, 세턴 송수신 파이프 저장
        self.IN_ORBIT[satellite.IDENTIFY_CODE] = {
            "MODULE": satellite,
            "PROCESS": satellite_process,
            "RESPONSE_CONN": planet_res,
            "REQUEST_CONN": planet_req
        }


if __name__ == '__main__':
    # 멀티프로세스 시작점 프리즈
    multiprocessing.freeze_support()
    
    # 세턴 생성
    SATURN = Saturn()
    
    # 종료 배열 (작업 종료된 위성 저장)
    IS_LANDED = []
    
    while True:
        try:
            # 작업이 필요없는 자원 위성 카운트
            _res_satellite_idle = 0
            
            # 궤도 배열 스캔 (작업 중인 위성 처리 시작)
            for _id, _satellite in SATURN.IN_ORBIT.items():
                print("====================================================================================================")
                
                # 프로세스가 살아 있는지 확인
                if _satellite['PROCESS'].is_alive():
                    print("Waiting for Message from {}.".format(_id))
                    
                    # 10초간 위성이 보내는 메시지를 수신 대기
                    # 10초 안에 메시지 수신 시, 딜레이 없이 return true
                    # 10초가 지나면 return false
                    _is_response = _satellite['RESPONSE_CONN'].poll(10)
                    
                    # 수신된 메시지 확인
                    if _is_response:
                        try:
                            # 수신 객체 확인
                            _response = _satellite['RESPONSE_CONN'].recv()
                            
                            # 메시지, 시간 확인
                            _message = _response['MESSAGE']
                            _send_time = _response['SEND_TIME']

                            # 출력
                            print("{} is in Orbit.".format(_id))
                            print("Message from {} ----------".format(_id))
                            print("Message   : {}".format(_message))
                            print("Send Time : {}".format(_send_time))
                            
                            # 작업 시작 메시지 일 경우,
                            if _message == "Do Work.":
                                # 발사대 배열에서 해당 위성 삭제
                                if _id in SATURN.IN_LAUNCHER:
                                    SATURN.IN_LAUNCHER.remove(_id)
                                    
                            # 작업이 없는 경우,
                            elif _message == "Nothing Work.":
                                # 작업이 필요없는 자원 위성 카운트
                                _res_satellite_idle += 1
                                
                        except EOFError:
                            # 프로세스가 종료 시 파이프도 같이 닫히는데,
                            # 이렇게 닫힌 파이프의 경우 EOFError 를 발생시킴.
                            pass
                        finally:
                            try:
                                # 위성에게 작업 속행 신호 발신
                                _satellite['REQUEST_CONN'].send(True)
                            except:
                                SATURN.logging(sys.exc_info(), "{} is out of control.".format(_id))
                    
                    # 수신된 메시지가 없을 경우
                    else:
                        print("Noting to Message from {}.".format(_id))
                        pass
                    
                # 프로세스가 죽었을 경우
                else:
                    print("{} has landed.".format(_id))
                    # 프로세스 안전 종료 처리
                    _satellite['PROCESS'].join()
                    
                    # 종료 배열에 해당 위성 추가
                    IS_LANDED.append(_id)
                    
            # 모든 자원 위성이 할 일이 없을 때,
            if len(SATURN.RESOURCE_SATELLITE) == _res_satellite_idle:
                # 모든 위성을 발사대에서 해체
                SATURN.IN_LAUNCHER.clear()
                
            # 종료된 위성이 있을 경우,
            if len(IS_LANDED) > 0:
                # 궤도 배열에서 해당 위성 삭제
                for _id in IS_LANDED:
                    SATURN.IN_ORBIT.__delitem__(_id)
                    
        # 위성 메세지 수신 중 오류 발생 처리
        except:
            SATURN.logging(sys.exc_info())
            
        # 오류 없이 정상 처리되었을 경우,
        else:
            # 궤도(작업 진행 중)에 위성이 없을 경우,
            if len(SATURN.IN_ORBIT) == 0:
                
                # 발사대(작업 대기 중)에 위성이 없을 경우,
                # 모든 위성이 그날의 일을 마쳤음.
                if len(SATURN.IN_LAUNCHER) == 0:
                    
                    # 날짜가 바뀌었는지 체크
                    # 날짜가 바뀌었으면 날짜 업데이트
                    if SATURN.DATE_MARK != SATURN.set_date_mark():
                        SATURN.DATE_MARK = SATURN.set_date_mark()
                        
                        print("New sun has risen.")
                        print("But it is not yet time to work.")

                        # 새로운 데이터가 추가되는 시간인지 확인 (13시)
                        while SATURN.set_time_mark()[0:2] != "13":
                            time.sleep(60)
                        
                        # 발사대에 자원 위성 장착
                        for _satellite in SATURN.RESOURCE_SATELLITE:
                            SATURN.IN_LAUNCHER.append(_satellite.IDENTIFY_CODE)
                            
                        # 발사대에 공급 위성 장착
                        for _satellite in SATURN.SUPPLY_SATELLITE:
                            SATURN.IN_LAUNCHER.append(_satellite.IDENTIFY_CODE)
                            
                    # 날짜가 바뀌지 않았을 경우,
                    else:
                        print("Saturn finished day's work.")
                        
                        # 날짜가 바뀔 때까지 대기
                        while SATURN.DATE_MARK == SATURN.set_date_mark():
                            time.sleep(60)
                
                # 발사대에 아직 위성이 남아있을 경우,
                else:
                    # 자원 위성 발사 카운트
                    # 자원 위성과 공급 위성이 동시에 궤도에 올라가면 안되기 때문에 발사된 위성이 있는지 카운트
                    _res_satellite_in_orbit = 0
                    
                    # 자원 위성 발사
                    for _satellite in SATURN.RESOURCE_SATELLITE:
                        if _satellite.IDENTIFY_CODE in SATURN.IN_LAUNCHER:
                            SATURN.launch(_satellite)
                            _res_satellite_in_orbit += 1
                        
                    # 발사된 자원 위성이 없을 경우,
                    if _res_satellite_in_orbit == 0:
                        
                        # 공급 위성 발사
                        for _satellite in SATURN.SUPPLY_SATELLITE:
                            if _satellite.IDENTIFY_CODE in SATURN.IN_LAUNCHER:
                                SATURN.launch(_satellite)
        finally:
            # 종료 배열 클리어
            IS_LANDED.clear()
            time.sleep(0.1)
