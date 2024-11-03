from google.cloud import secretmanager, bigquery
import json
import streamlit as st

# 1. Secret Manager에서 서비스 계정 키를 불러오는 함수
def get_service_account_key(secret_id="moviejj", project_id="semiotic-vial-440207-q4"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    service_account_info = response.payload.data.decode("UTF-8")
    return json.loads(service_account_info)

# 2. BigQuery 클라이언트 설정 함수
def get_bq_client():
    service_account_key = get_service_account_key()
    client = bigquery.Client.from_service_account_info(service_account_key)
    return client

# 3. BigQuery에서 데이터 조회 함수
def load_data(query):
    client = get_bq_client()
    query_job = client.query(query)
    return query_job.result().to_dataframe()

# 4. BigQuery에 JSON 데이터를 업데이트하는 함수
def update_bigquery_table(data):
    client = get_bq_client()
    table_id = "semiotic-vial-440207-q4.json_test.usertest"  # BigQuery 테이블 ID 반영
    errors = client.insert_rows_json(table_id, [data])  # JSON 형식으로 데이터를 리스트에 담아 전달
    if errors == []:
        st.success("데이터가 성공적으로 업데이트되었습니다.")
    else:
        st.error(f"업데이트 중 오류 발생: {errors}")

# 5. Streamlit 인터페이스 설정
# BigQuery에서 데이터를 조회하고 화면에 표시
query = "SELECT * FROM `semiotic-vial-440207-q4.json_test.usertest` LIMIT 100"  # 쿼리를 원하는 대로 변경
data = load_data(query)
st.write("빅쿼리 테이블 데이터", data)

# JSON 데이터를 사용자로부터 입력받기
json_input = st.text_area("업데이트할 JSON 데이터를 입력하세요", '{"name": "Alice", "age": 30}')
updated_data = json.loads(json_input)

# 업데이트 버튼을 클릭했을 때 BigQuery 테이블에 업데이트 수행
if st.button("데이터 업데이트"):
    update_bigquery_table(updated_data)
