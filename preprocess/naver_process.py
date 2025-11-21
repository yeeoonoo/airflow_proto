from pandas import json_normalize

__all__ = ['preprocessing']

def preprocessing(ti):
    # ti: Task Instance - dag 내의 task의 정보를 얻을 수 있는 객체

    # xcom(cross-communication) - Operators 간에 데이터를 주고받을 수 있는 도구
    search_result = ti.xcom_pull(task_ids = ['crawl_naver'])

    # xcom을 이용해 가지고 온 결과가 없는 경우
    if not len(search_result):
        raise ValueError('검색결과 없음')
    
    items = search_result[0]['items']
    processed_items = json_normalize([
        {
            'title': item['title'],
            'address': item['address'],
            'category': item['category'],
            'description': item['description'],
            'link': item['link'],
        }
        for item in items
    ])

    processed_items.to_csv('../data/naver_preprocessed.csv', index = False, header=False)