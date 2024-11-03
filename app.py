from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
import json

# BigQuery 클라이언트 생성 함수
def get_bq_client():
    # Streamlit Secrets Manager에서 서비스 계정 키를 불러옵니다
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id, location="US")  # 위치를 US로 설정
    return client

# 데이터 조회 함수
def load_data(query):
    client = get_bq_client()
    query_job = client.query(query)
    return query_job.result().to_dataframe()

# JSON 데이터를 BigQuery에 업데이트하는 함수
def update_bigquery_table(data):
    client = get_bq_client()
    table_id = "semiotic-vial-440207-q4.json_test.usertest"  # 테이블 ID를 확인된 경로로 설정
    errors = client.insert_rows_json(table_id, [data])  # JSON 형식으로 데이터를 리스트에 담아 전달
    if errors == []:
        st.success("데이터가 성공적으로 업데이트되었습니다.")
    else:
        st.error(f"업데이트 중 오류 발생: {errors}")

# Streamlit 인터페이스 설정
# BigQuery에서 데이터를 조회하고 화면에 표시
query = "SELECT * FROM `semiotic-vial-440207-q4.json_test.usertest` LIMIT 100"  # 확인된 테이블 ID로 설정
data = load_data(query)
st.write("빅쿼리 테이블 데이터", data)

# JSON 데이터를 사용자로부터 입력받기
json_input = st.text_area("업데이트할 JSON 데이터를 입력하세요", '{"name": "Alice", "age": 30}')
updated_data = json.loads(json_input)

# 업데이트 버튼을 클릭했을 때 BigQuery 테이블에 업데이트 수행
if st.button("데이터 업데이트"):
    update_bigquery_table(updated_data)
