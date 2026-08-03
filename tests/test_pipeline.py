"""
프로그램명: Day 1 데이터 수집 미니 파이프라인 테스트
설명: Pydantic 검증과 CSV·Parquet 저장 기능을 단위 테스트한다.
작성자: 김지성
변경내용:
- Step 4: 정상·범위 오류·배열 길이 오류에 대한 모델 테스트를 추가했다.
- Step 5: 정규화와 CSV·Parquet 저장 결과 테스트를 추가했다.
- Step 6: 외부 API를 호출하지 않는 pytest 테스트를 구성했다.
"""

import pytest
from pydantic import ValidationError

from main import (
    WeatherResponse,
    benchmark_storage,
    ensure_complete_datasets,
    normalize_validated_data,
)


def valid_weather_data() -> dict[str, object]:
    """테스트에서 공통으로 사용할 정상 날씨 응답을 반환한다."""
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


# Step 4: 정상 응답과 잘못된 범위·배열 관계를 각각 검증한다.
def test_weather_response_accepts_valid_data() -> None:
    """정상 날씨 응답은 Pydantic 검증을 통과해야 한다."""
    weather = WeatherResponse.model_validate(valid_weather_data())

    assert weather.timezone == "Asia/Seoul"
    assert len(weather.hourly.time) == 2


def test_weather_response_rejects_invalid_probability() -> None:
    """강수확률이 0~100 범위를 벗어나면 검증에 실패해야 한다."""
    data = valid_weather_data()
    data["hourly"]["precipitation_probability"] = [20, 101]

    with pytest.raises(ValidationError):
        WeatherResponse.model_validate(data)


def test_weather_response_rejects_mismatched_arrays() -> None:
    """시간·기온·강수확률 배열 길이가 다르면 검증에 실패해야 한다."""
    data = valid_weather_data()
    data["hourly"]["temperature_2m"] = [27.1]

    with pytest.raises(ValidationError):
        WeatherResponse.model_validate(data)


def test_weather_response_rejects_invalid_time() -> None:
    """날짜·시간 형식이 아니면 Pydantic 검증에 실패해야 한다."""
    data = valid_weather_data()
    data["hourly"]["time"] = ["잘못된 시간", "2026-08-03T01:00"]

    with pytest.raises(ValidationError):
        WeatherResponse.model_validate(data)


# Step 5: 검증 데이터를 두 파일 형식으로 저장하고 측정 결과를 확인한다.
def test_normalize_and_store_weather_data(tmp_path) -> None:
    """정규화한 날씨 데이터를 CSV와 Parquet으로 저장해야 한다."""
    weather = WeatherResponse.model_validate(valid_weather_data())
    results = [("Open-Meteo", weather, None)]
    datasets = normalize_validated_data(results)

    measurements = benchmark_storage(datasets, tmp_path)

    assert set(measurements["format"]) == {"CSV", "PARQUET"}
    assert (tmp_path / "weather.csv").exists()
    assert (tmp_path / "weather.parquet").exists()


def test_rejects_incomplete_datasets(tmp_path) -> None:
    """API 누락 시 이전 생성 파일을 제거하고 검증에 실패해야 한다."""
    stale_file = tmp_path / "weather.csv"
    unrelated_file = tmp_path / "memo.txt"
    stale_file.write_text("이전 실행 결과", encoding="utf-8")
    unrelated_file.write_text("사용자 파일", encoding="utf-8")

    with pytest.raises(RuntimeError, match="필수 데이터셋 누락"):
        ensure_complete_datasets({}, tmp_path)

    assert not stale_file.exists()
    assert unrelated_file.exists()
