# 필요한 모듈 Import
from datetime import datetime
from airflow import DAG
import json
from naver_preprocess import preprocessing

# 사용할 Operator Import
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensor.http import httpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

# 디폴트 설정
default_args = {
    'start_date': datetime(2025, 1, 1) # 2026/1/1부터 대그 시작 -> 현재는 25년 11월이므로 대그를 실행하면 무조건 한 번 돌아갈 것
}

# 발급받은 네이버 API 키
NAVER_CLI_ID = 'd4FJJVO631NoPx_MRXly'
NAVER_CLI_SECRET = 'MjbClZn0HY'

def _complete():
    print('네이버 검색 DAG 완료!')

with DAG(
    dag_id='naver_search_pipeline',
    # crontab 표현 사용 가능 https://crontab.guru/
    schedule_interval = '@daily',
    default_args = default_args,
    # 태그는 자유롭게 지정 가능
    tags=['naver', 'search', 'api'],
    # catchup을 True로 설정하면 start_date부터 현재까지의 모든 스케줄을 백필함
    catchup=False
    ) as dag:

    # 네이버 API로 지역 식당을 검색
    # 지역 식당명, 주소, 카테고리, 설명, 링크를 저장
    creating_table = SqliteOperator(
        task_id = 'creating_table',
        sqlite_conn_id = 'db_sqlite', # 웹UI에서 connection ID 등록 필요
        # naver_seatch_result 테이블이 없으면 생성
        sql = '''
            CREATE TABLE IF NOT EXISTS naver_search_result (
                title TEXT,
                category TEXT,
                address TEXT,
                description TEXT,
                link TEXT
            )
        '''
    )

    # http 센서를 이용해 응답 확인 (감지하는 오퍼레이터로 실제 데이터를 가져오지 않음)
    is_api_available = httpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'naver_search_api',
        endpoint = 'v1/search/local.json', # url -uri에서 Host 부분을 제외한 파트]
        # 요청 헤더, -H 다음에 오는 내용들
        headers = {
            'X-Naver-Client-Id': f'{NAVER_CLI_ID}',
            'X-Naver-Client-Secret': f'{NAVER_CLI_SECRET}',
        },
        request_params = {
            'query' : '김치찌개',
            'display' : 5
        }, # 요청 파라미터
        response_check = lambda response: response.json()
    )

    # 네이버 검색 결과를 가져올 오퍼레이터
    crawl_naver = SimpleHttpOperator(
        task_id = 'crawl_naver',
        http_conn_id = 'naver_search_api',
        endpoint = 'v1/search/local.json',
        headers = {
            'X-Naver-Client-Id': f'{NAVER_CLI_ID}',
            'X-Naver-Client-Secret': f'{NAVER_CLI_SECRET}',
        },
        data = {
            'query' : '김치찌개',
            'display' : 5
        },
        method = 'GET', # 통신방식 GET, POST 등 맞는 것으로
        response_filter = lambda res : json.loads(res.text),
        log_response = True
    )

    # 검색결과 전처리 및 CSV 저장
    preprocess_result = PythonOperator(
        task_id = 'preprocess_result',
        python_callable = preprocessing #실행할 파이썬 함수
    )
    
    # CSV 파일로 저장된 것을 테이블에 저장
    store_result = BashOperator(
        task_id = 'store_result',
        bash_command = 'echo -e ".separator ","\n.import /home/kurran/airflow/dags/data/naver_processed_result.csv naver_search_result" | sqlite3 /home/kurran/airflow/airflow.db'
    )

    # 대그 완료 출력
    print_complete = PythonOperator(
        task_id = 'print_complete',
        python_callable = _complete #실행할 파이썬 함수
    )

    # 파이프라인 구성하기
    creating_table >> is_api_available >> crawl_naver >> preprocess_result >> print_complete