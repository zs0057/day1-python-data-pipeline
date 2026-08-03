"""
프로그램명: 데이터 수집 미니 파이프라인 테스트
설명: Pydantic 검증과 CSV·Parquet 저장 기능을 단위 테스트한다.

변경 내역
- 2026-08-03: Step 2 Pydantic 정상·오류 데이터 테스트 구성
- 2026-08-03: Step 5 정규화 및 CSV·Parquet 저장 테스트 구성
- 2026-08-03: Step 5 필수 데이터셋 누락 시 안전 처리 테스트 구성
- 2026-08-03: Step 7 대용량 성능 비교 데이터 생성 테스트 구성
"""

import pytest
from pydantic import ValidationError

from main import (
    WeatherResponse,
    benchmark_storage,
    create_large_benchmark_dataset,
    ensure_complete_datasets,
    normalize_validated_data,
)


# ==================================================
# Step 2. Pydantic v2 스키마 테스트 데이터
# ==================================================
def valid_weather_data() -> dict[str, object]:
    """테스트에서 공통으로 사용할 정상 날씨 응답을 반환한다."""
    # 실제 API를 호출하지 않아도 같은 입력으로 반복 가능한 단위 테스트를 수행한다.
    return {
        "latitude": 37.5665,
        "longitude": 126.9780,
        "timezone": "Asia/Seoul",
        "hourly": {
            "time": ["2026-08-03T00:00", "2026-08-03T01:00"],
            "temperature_2m": [27.1, 26.8],
            "precipitation_probability": [20, 30],
        },
    }


# ==================================================
# Step 2. Pydantic 정상·오류 조건 테스트
# ==================================================
def test_weather_response_accepts_valid_data() -> None:
    """정상 날씨 응답은 Pydantic 검증을 통과해야 한다."""
    weather = WeatherResponse.model_validate(valid_weather_data())

    assert weather.timezone == "Asia/Seoul"
    assert len(weather.hourly.time) == 2


def test_weather_response_rejects_invalid_probability() -> None:
    """강수확률이 0~100 범위를 벗어나면 검증에 실패해야 한다."""
    data = valid_weather_data()
    # 두 번째 강수확률을 허용 최댓값보다 큰 101로 변경한다.
    data["hourly"]["precipitation_probability"] = [20, 101]

    with pytest.raises(ValidationError):
        WeatherResponse.model_validate(data)


def test_weather_response_rejects_mismatched_arrays() -> None:
    """시간·기온·강수확률 배열 길이가 다르면 검증에 실패해야 한다."""
    data = valid_weather_data()
    # 기온 배열만 한 건으로 줄여 모델 수준의 관계 검증을 확인한다.
    data["hourly"]["temperature_2m"] = [27.1]

    with pytest.raises(ValidationError):
        WeatherResponse.model_validate(data)


def test_weather_response_rejects_invalid_time() -> None:
    """날짜·시간 형식이 아니면 Pydantic 검증에 실패해야 한다."""
    data = valid_weather_data()
    # ISO 날짜·시간이 아닌 한글 문자열이 datetime 검증에서 차단되는지 확인한다.
    data["hourly"]["time"] = ["잘못된 시간", "2026-08-03T01:00"]

    with pytest.raises(ValidationError):
        WeatherResponse.model_validate(data)


# ==================================================
# Step 5. DataFrame 변환·파일 저장 테스트
# ==================================================
def test_normalize_and_store_weather_data(tmp_path) -> None:
    """정규화한 날씨 데이터를 CSV와 Parquet으로 저장해야 한다."""
    weather = WeatherResponse.model_validate(valid_weather_data())
    results = [("Open-Meteo", weather, None)]
    datasets = normalize_validated_data(results)

    # pytest의 tmp_path를 사용해 실제 프로젝트 data 폴더를 변경하지 않는다.
    measurements = benchmark_storage(datasets, tmp_path)

    # 두 형식의 측정 결과와 실제 생성 파일을 함께 확인한다.
    assert set(measurements["format"]) == {"CSV", "PARQUET"}
    assert (tmp_path / "weather.csv").exists()
    assert (tmp_path / "weather.parquet").exists()


def test_rejects_incomplete_datasets(tmp_path) -> None:
    """API 누락 시 이전 생성 파일을 제거하고 검증에 실패해야 한다."""
    # 프로그램 결과 파일과 사용자가 만든 파일이 함께 있는 상황을 구성한다.
    stale_file = tmp_path / "weather.csv"
    stale_large_file = tmp_path / "weather_large.parquet"
    unrelated_file = tmp_path / "memo.txt"
    stale_file.write_text("이전 실행 결과", encoding="utf-8")
    stale_large_file.write_text("이전 대용량 결과", encoding="utf-8")
    unrelated_file.write_text("사용자 파일", encoding="utf-8")

    with pytest.raises(RuntimeError, match="필수 데이터셋 누락"):
        ensure_complete_datasets({}, tmp_path)

    # 프로그램 결과만 제거되고 관련 없는 사용자 파일은 보존돼야 한다.
    assert not stale_file.exists()
    assert not stale_large_file.exists()
    assert unrelated_file.exists()


# ==================================================
# Step 7. 추가 기능 - 대용량 데이터 생성 테스트
# ==================================================
def test_create_large_benchmark_dataset() -> None:
    """원본 날씨 데이터를 지정한 행 수로 정확히 확장해야 한다."""
    weather = WeatherResponse.model_validate(valid_weather_data())
    datasets = normalize_validated_data([("Open-Meteo", weather, None)])

    # 단위 테스트에서는 실행 속도를 위해 1,000행만 생성한다.
    large_weather = create_large_benchmark_dataset(
        datasets["weather"],
        target_rows=1_000,
    )

    assert len(large_weather) == 1_000
    assert large_weather["record_id"].is_unique
    assert large_weather["record_id"].iloc[0] == 1
    assert large_weather["record_id"].iloc[-1] == 1_000
