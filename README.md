# 데이터 수집 미니 파이프라인

- 프로그램명: 데이터 수집 미니 파이프라인
- 설명: 세 공개 API의 데이터를 비동기로 수집하고 검증·저장·성능 비교를 수행합니다.
- 작성자: 김지성

## 변경 내용

- Step 1: 제출용 프로젝트 구조를 구성했습니다.
- Step 2: 가상환경과 `requirements.txt`를 구성했습니다.
- Step 3: `asyncio.gather()`와 `httpx`를 이용한 비동기 수집을 구현했습니다.
- Step 4: Pydantic v2 모델로 타입·범위·배열 길이를 검증합니다.
- Step 5: 검증 데이터를 CSV·Parquet으로 저장하고 읽기·쓰기 시간을 비교합니다.
- Step 6: 외부 API를 호출하지 않는 pytest 테스트와 실행 방법을 정리했습니다.
- Step 7: 검증된 날씨 데이터를 100,000행으로 확장해 대용량 성능을 비교합니다.

## 사용 API

- Open-Meteo: 서울 3일 시간별 기온·강수확률
- Countries.dev: 대한민국 국가 정보
- ip-api: `8.8.8.8`의 IP 기반 지역 정보

## 프로젝트 구조

```text
판교_5반_김지성_day1종합실습/
├── data/                  # 실행 시 생성되는 CSV·Parquet 파일
├── tests/
│   └── test_pipeline.py   # Pydantic·저장 기능 테스트
├── .gitignore
├── main.py                # 수집·검증·저장·성능 비교 파이프라인
├── pyproject.toml         # pytest 실행 경로 설정
├── README.md
└── requirements.txt
```

## 설치 및 실행

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python main.py
```

실행하면 세 API의 수집·검증 결과를 출력하고 `data/`에 다음 파일을 생성합니다.

```text
weather.csv
weather.parquet
country.csv
country.parquet
ip_location.csv
ip_location.parquet
weather_large.csv
weather_large.parquet
```

마지막에는 원본 74행과 성능 실험용 100,000행 데이터의 CSV·Parquet 쓰기 시간, 읽기 시간, 파일 크기를 비교해 출력합니다. 대용량 데이터는 실제 API 추가 수집이 아니라 검증된 날씨 데이터를 확장한 성능 비교용 데이터입니다.

## 테스트 및 코드 검사

```bash
pytest
ruff check .
```

테스트에서는 외부 API를 호출하지 않고 준비된 샘플 데이터로 다음 항목을 확인합니다.

- 정상 날씨 데이터 검증
- 잘못된 강수확률 범위 예외 처리
- 잘못된 날짜·시간 형식 예외 처리
- 시간·기온·강수확률 배열 길이 불일치 예외 처리
- 필수 데이터셋 누락 시 저장 중단
- CSV·Parquet 파일 생성
- 지정한 행 수로 대용량 성능 비교 데이터를 확장하는 기능

## 주의사항

- `ip-api`는 HTTP 엔드포인트이므로 네트워크 정책에 따라 차단될 수 있습니다.
- 공개 API 상태에 따라 요청이 일시적으로 실패할 수 있습니다.
- 필수 API가 누락되면 이전 실행에서 생성한 CSV·Parquet을 제거하고 저장을 중단합니다.
- `weather_large` 파일은 저장 형식의 규모별 성능 비교를 위한 실험용 데이터입니다.
- `data/*.csv`, `data/*.parquet`, `.venv/`는 Git 추적 대상에서 제외됩니다.
