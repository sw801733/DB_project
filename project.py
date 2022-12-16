import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import requests
from datetime import datetime

conn = sqlite3.connect('C:/Users/Sunwoo/Desktop/DB_project/bus_logbook.db')
curs = conn.cursor()

# sqlite 에 버스 운행 일지를 저장할 table 만들기
def Create_Table(curs):
    curs.execute("CREATE TABLE if not exists logbook( \
                    노선ID text, \
                    차량ID text, \
                    노선번호 text, \
                    버스번호 text, \
                    정류소명 text, \
                    상행하행 text, \
                    도착 text defalut '0', \
                    출발 text defalut '0')")

# 버스 운행 일지 csv 파일을 sqlite table 에 INSERT
def csv_db(curs):
    bus_logbook = pd.read_csv('C:/Users/Sunwoo/Desktop/DB_project/logbook_data.csv',delimiter=',' , encoding='cp949')
    bus_logbook.to_sql('logbook', conn, if_exists='replace', index=False)


# table 생성과 csv 파일 insert
# create_table(curs)
# csv_db(curs)
# conn.commit()


# 인천광역시 버스 노선 조회 API 를 통해 버스번호(busNumber)에 해당하는
# 노선 번호(routeID)를 통해 해당 버스의 출발 정류소, 최소 최대 배차 간격 정보를 얻는다.
def BUS_INFO(routeID):
    url = 'http://apis.data.go.kr/6280000/busRouteService/getBusRouteId'
    params ={'serviceKey' : '3MGCsqU1s37+FThv9rBexEJF6T3FEyfVylGYVL/r8lDJ+xZgDIC2uGnf4SHXImWbd144js06L5sjzyFlcfZjFA==', 'pageNo' : '1', 'numOfRows' : '10', 'routeID' : routeID }

    response = requests.get(url, params=params)
    soup = BeautifulSoup(response.content, features='xml')
    data = soup.find_all('itemList')

    for item in data:
        RouteNo = item.find('ROUTENO').get_text()
        max_ALLOCGAP = int(item.find('MAX_ALLOCGAP').get_text())
        min_ALLOCGAP = int(item.find('MIN_ALLOCGAP').get_text())
        origin_BSTOPID = item.find('ORIGIN_BSTOPID').get_text()
        origin_BSTOPNM = item.find('ORIGIN_BSTOPNM').get_text()
        
        # print(RouteNo)
        # print(origin_BSTOPID)
        # print(origin_BSTOPNM)
        # print(max_ALLOCGAP)
        # print(min_ALLOCGAP)
        
    return max_ALLOCGAP, min_ALLOCGAP, RouteNo, origin_BSTOPID, origin_BSTOPNM

# 인천광역시 버스위치정보 조회 API 를 이용해 타고자 하는 버스의 현재 위치를 가져온다.
# 이때 가져올 정보는 가장 최근에 시작점에서 출발한 버스이므로 최근 정류소순번 (LATEST_STOPSEQ)가 
# 가장 작은 버스의 위치 (LATEST_STOP_NAME)를 가져온다.
def BUS_Current_Location(routeID):
    url = 'http://apis.data.go.kr/6280000/busLocationService/getBusRouteLocation'
    params ={'serviceKey' : '3MGCsqU1s37+FThv9rBexEJF6T3FEyfVylGYVL/r8lDJ+xZgDIC2uGnf4SHXImWbd144js06L5sjzyFlcfZjFA==', 'pageNo' : '1', 'numOfRows' : '100', 'routeID' : routeID }

    response = requests.get(url, params=params)
    soup = BeautifulSoup(response.content, features='xml')
    data = soup.find_all('itemList')
    
    LATEST_STOPSEQ = 0
    LATEST_STOP_ID = 0
    LATEST_STOP_NAME = 0
    min_STOPSEQ = 1000
    DIRCD = 0
    
    for item in data:
        DIRCD = item.find('DIRCD').get_text()
        LATEST_STOPSEQ = item.find('LATEST_STOPSEQ').get_text()
        if DIRCD == '0' and int(LATEST_STOPSEQ) < min_STOPSEQ:
            LATEST_STOP_ID = item.find('LATEST_STOP_ID').get_text()
            LATEST_STOP_NAME = item.find('LATEST_STOP_NAME').get_text()
            min_STOPSEQ = int(LATEST_STOPSEQ)
                
    # print(min_STOPSEQ)
    # print(LATEST_STOP_ID)
    # print(LATEST_STOP_NAME)
        
    return DIRCD, LATEST_STOP_ID, LATEST_STOP_NAME


# 입력한 버스번호를 가진 버스의 다음 시작점 도착 예정 시간을 계산한다. 
# 버스의 정류소별 출발과 도착 시간을 포함한 운행일지가 들어간 테이블 logbook 을 통해
# 이전 버스들의 시작점과 이후 정류소별 출발 시간을 비교하여 평균 소요 시간을 구한다.
# 이는 결국 운행일지를 통해 얻어낸 시작점에서 해당 정류소까지 걸린 평균 시간이 된다.
# 이때 시작점과 비교하는 정류소는 가장 최근에 시작점에서 출발한 버스의 현재 정류소만 사용한다.
# 시작점에서 해당 정류소까지의 걸린 평균 시간과 최대 최소 배차 간격의 차를 통해 다음 시작점에 도착하는 버스의 예상 시간을 구한다.
def Cal_Estimated_Arrival_Time(busNumber, origin_BSTOPNM, LATEST_STOP_NAME, min_ALLOCGAP, max_ALLOCGAP):
    start_STOP_time = []
    next_STOP_time = []

    result1 = curs.execute(f"select 출발 from logbook where 노선번호 = '{busNumber}' and 상행하행 = '상행' and 정류소명 = ?", (origin_BSTOPNM,))
    for row in result1:
        start_STOP_time.append(datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S'))
        
    result2 = curs.execute(f"select 출발 from logbook where 노선번호 = '{busNumber}' and 상행하행 = '상행' and 정류소명 = ?", (LATEST_STOP_NAME,))
    for row2 in result2:
        next_STOP_time.append(datetime.strptime(row2[0], '%Y-%m-%d %H:%M:%S'))


    total_time = 0

    for i in range(len(start_STOP_time)):
        # 잘못된 데이터셋이 잘못 들어가 있는 것을 막기 위해 하루 차이 이상 나는 경우는 제외한다.
        if '-1 day' not in str(next_STOP_time[i] - start_STOP_time[i]):
            total_time = total_time + next_STOP_time[i].minute - start_STOP_time[i].minute

    mean_time = total_time / len(start_STOP_time)
    min_estimated_arrival_time =  min_ALLOCGAP - mean_time
    max_estimated_arrival_time =  max_ALLOCGAP - mean_time

    return min_estimated_arrival_time, max_estimated_arrival_time


# 시작점 도착 예정 시간을 검색하고자 하는 버스를 입력하고
# 최대 예상 시간과 최소 예상 시간을 출력한다. 
def main():
    busNumber = input("검색하고자 하는 버스 번호를 입력하세요 : ")
    
    # 입력한 버스 번호에 대한 노선 번호를 가져온다.
    result = curs.execute(f"select distinct 노선ID from logbook where 노선번호 = '{busNumber}'")
    routeID = 0
    for row in result:
        routeID = row[0]

    # 입력한 버스의 배차 간격과 시작점을 가져오고 가장 최근에 시작점을 지난 버스의 현재 정류소 위치도 가져온다.
    max_ALLOCGAP, min_ALLOCGAP, RouteNo, origin_BSTOPID, origin_BSTOPNM = BUS_INFO(routeID)
    DIRCD, LATEST_STOP_ID, LATEST_STOP_NAME = BUS_Current_Location(routeID)


    print("버스 노선의 시작 위치 : ", origin_BSTOPNM)
    
    # 현재 운행 중인 버스의 정류소가 없는 경우는 운행 중이 아니므로 이에 대한 조건문
    if LATEST_STOP_NAME == 0 :
        print("현재 운행 중인 버스가 없습니다!")
        return
    
    print("현재 버스의 위치 : " , LATEST_STOP_NAME)


    # 시작점에 도착할 다음 버스의 예상 시간을 계산한다.
    min_estimated_arrival_time, max_estimated_arrival_time = Cal_Estimated_Arrival_Time(busNumber, origin_BSTOPNM, LATEST_STOP_NAME, min_ALLOCGAP, max_ALLOCGAP)

    min_arrival_minute = int(min_estimated_arrival_time)
    max_arrival_minute = int(max_estimated_arrival_time)
    print("시작점 도착 최소 예상 시간 : ", min_arrival_minute, "분") 
    print("시작점 도착 최대 예상 시간 : ", max_arrival_minute, "분") 


if __name__ == '__main__':
    main()


