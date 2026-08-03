"""
프로그램명: 데이터 수집 미니 파이프라인
설명: 세 공개 API를 비동기로 수집하고 검증·저장·성능 비교를 수행한다.

변경 내역
- 2026-08-03: Step 1 API 및 실행 환경 설정
- 2026-08-03: Step 2 Pydantic v2 스키마 정의
- 2026-08-03: Step 3 비동기 API 수집 및 오류 처리
- 2026-08-03: Step 4 수집 데이터 검증
- 2026-08-03: Step 5 DataFrame 변환·저장·성능 비교
- 2026-08-03: Step 6 전체 파이프라인 실행
- 2026-08-03: Step 7 추가 기능 - 대용량 데이터 성능 비교
"""

import asyncio
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Annotated, Literal, Self

import httpx
import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    IPvAnyAddress,
    ValidationError,
    model_validator,
)

# ==================================================
# Step 1. API 및 실행 환경 설정
# ==================================================
# 교재에서 지정한 공개 API 3개의 이름과 요청 URL을 한 곳에서 관리한다.
# 딕셔너리의 입력 순서는 asyncio.gather() 결과와 출력 순서를 일정하게 유지한다.
API_ENDPOINTS = {
    # 서울시청 좌표를 기준으로 3일간 시간별 기온과 강수확률을 요청한다.
    "Open-Meteo": (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=37.5665"
        "&longitude=126.9780"
        "&hourly=temperature_2m,precipitation_probability"
        "&forecast_days=3"
        "&timezone=Asia/Seoul"
    ),
    "Countries.dev": "https://countries.dev/alpha/KOR",
    "ip-api": "http://ip-api.com/json/8.8.8.8",
}

# 각 처리 단계는 데이터 또는 오류 메시지 중 하나를 반환한다.
# 튜플의 첫 번째 값인 API 이름을 유지하여 결과와 원인을 구분한다.
FetchResult = tuple[str, dict[str, object] | None, str | None]
ValidatedResult = tuple[str, BaseModel | None, str | None]

# 세 API가 모두 검증됐을 때만 저장하기 위한 필수 데이터셋 이름이다.
EXPECTED_DATASETS = frozenset({"weather", "country", "ip_location"})
# 대용량 성능 실험 파일까지 포함한 프로그램 생성 파일 이름이다.
GENERATED_DATASETS = EXPECTED_DATASETS | {"weather_large"}


# ==================================================
# Step 2. Pydantic v2 스키마 정의
# ==================================================
# Annotated와 Field를 결합해 여러 모델에서 공통으로 사용할 값 범위를 정의한다.
Latitude = Annotated[float, Field(ge=-90, le=90)]
Longitude = Annotated[float, Field(ge=-180, le=180)]
Probability = Annotated[int, Field(ge=0, le=100)]


class ApiModel(BaseModel):
    """공개 API 응답 모델의 공통 설정이다."""

    # 필요한 필드만 검증하고 API가 추가로 제공하는 필드는 안전하게 무시한다.
    # populate_by_name=True는 Python 필드명과 API 별칭을 모두 입력에 허용한다.
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class HourlyWeather(ApiModel):
    """Open-Meteo의 시간별 날씨 데이터이다."""

    # Pydantic이 ISO 문자열을 datetime으로 변환하면서 날짜 형식도 함께 검증한다.
    time: list[datetime] = Field(min_length=1)
    temperature_2m: list[float] = Field(min_length=1)
    # 각 강수확률은 위에서 정의한 0~100 범위를 만족해야 한다.
    precipitation_probability: list[Probability] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_array_lengths(self) -> Self:
        """시간·기온·강수확률 배열의 길이가 같은지 검증한다."""
        # 같은 인덱스의 시간·기온·강수확률이 한 행으로 결합되므로 길이가 같아야 한다.
        lengths = {
            len(self.time),
            len(self.temperature_2m),
            len(self.precipitation_probability),
        }
        if len(lengths) != 1:
            raise ValueError("시간·기온·강수확률 배열의 길이가 다릅니다.")
        return self


class WeatherResponse(ApiModel):
    """Open-Meteo 응답에서 필요한 필드를 검증한다."""

    latitude: Latitude
    longitude: Longitude
    timezone: str = Field(min_length=1)
    hourly: HourlyWeather


class Currency(ApiModel):
    """국가의 통화 정보이다."""

    code: str = Field(pattern=r"^[A-Z]{3}$")
    name: str = Field(min_length=1)
    symbol: str | None = None


class Language(ApiModel):
    """국가의 언어 정보이다."""

    name: str = Field(min_length=1)
    iso639_1: str | None = Field(default=None, pattern=r"^[a-z]{2}$")


class CountryResponse(ApiModel):
    """Countries.dev 응답에서 필요한 필드를 검증한다."""

    name: str = Field(min_length=1)
    # API의 camelCase 필드를 Python의 snake_case 속성으로 매핑한다.
    alpha_2_code: str = Field(alias="alpha2Code", pattern=r"^[A-Z]{2}$")
    alpha_3_code: str = Field(alias="alpha3Code", pattern=r"^[A-Z]{3}$")
    capital: str = Field(min_length=1)
    region: str = Field(min_length=1)
    population: int = Field(gt=0)
    area: float = Field(gt=0)
    currencies: list[Currency] = Field(min_length=1)
    languages: list[Language] = Field(min_length=1)


class IpLocationResponse(ApiModel):
    """ip-api 응답에서 필요한 필드를 검증한다."""

    # Literal을 사용해 API가 명시적으로 success를 반환한 경우만 허용한다.
    status: Literal["success"]
    country: str = Field(min_length=1)
    country_code: str = Field(alias="countryCode", pattern=r"^[A-Z]{2}$")
    region_name: str = Field(alias="regionName", min_length=1)
    city: str = Field(min_length=1)
    latitude: Latitude = Field(alias="lat")
    longitude: Longitude = Field(alias="lon")
    timezone: str = Field(min_length=1)
    query: IPvAnyAddress


# API 이름을 해당 응답을 검증할 Pydantic 모델 클래스와 연결한다.
MODEL_BY_API: dict[str, type[BaseModel]] = {
    "Open-Meteo": WeatherResponse,
    "Countries.dev": CountryResponse,
    "ip-api": IpLocationResponse,
}


# ==================================================
# Step 3. 비동기 API 수집 및 오류 처리
# ==================================================
async def fetch_json(
    client: httpx.AsyncClient,
    api_name: str,
    url: str,
) -> FetchResult:
    """API 하나를 호출하고 이름, JSON 데이터, 오류 메시지를 반환한다."""
    try:
        # await는 네트워크 응답을 기다리는 동안 다른 API 요청이 실행되도록 제어권을 넘긴다.
        response = await client.get(url)
        # 4xx·5xx 응답을 HTTPStatusError로 변환해 정상 JSON과 구분한다.
        response.raise_for_status()
        # 응답 본문을 Python 객체로 변환한다. JSON 형식 오류는 아래에서 처리한다.
        data = response.json()

        # 이후 Pydantic 모델은 키-값 객체를 기대하므로 최상위 자료형을 먼저 확인한다.
        if not isinstance(data, dict):
            raise TypeError("최상위 JSON 형식이 객체가 아닙니다.")

        # ip-api는 HTTP 200에서도 status="fail"을 반환할 수 있다.
        if api_name == "ip-api" and data.get("status") == "fail":
            message = data.get("message", "알 수 없는 API 오류")
            return api_name, None, str(message)

        return api_name, data, None
    except httpx.TimeoutException:
        return api_name, None, "요청 시간이 초과되었습니다."
    except httpx.HTTPStatusError as error:
        return api_name, None, f"HTTP {error.response.status_code} 오류"
    except httpx.RequestError as error:
        return api_name, None, f"연결 오류: {error}"
    except (TypeError, ValueError) as error:
        return api_name, None, f"JSON 변환 오류: {error}"


async def collect_api_data() -> list[FetchResult]:
    """세 API를 asyncio.gather()로 동시에 호출한다."""
    # 연결·응답이 무한정 대기하지 않도록 전체 요청 제한 시간을 지정한다.
    timeout = httpx.Timeout(10.0)

    # 하나의 AsyncClient를 공유하면 API마다 연결 객체를 새로 만들 필요가 없다.
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        # 아직 실행 결과를 기다리지 않고 API별 코루틴을 먼저 구성한다.
        tasks = [
            fetch_json(client, api_name, url)
            for api_name, url in API_ENDPOINTS.items()
        ]
        # gather()가 세 코루틴을 동시에 실행하고 입력 순서대로 결과를 모은다.
        return list(await asyncio.gather(*tasks))


def print_collection_summary(results: list[FetchResult]) -> None:
    """수집 성공 여부와 응답의 최상위 필드를 간략하게 출력한다."""
    success_count = 0

    for api_name, data, error in results:
        # 오류 메시지가 있으면 실패 원인만 출력하고 다음 API 결과를 계속 확인한다.
        if error is not None:
            print(f"[실패] {api_name}: {error}")
            continue

        success_count += 1
        # 전체 JSON 대신 최상위 필드만 출력하여 응답 구조를 간단히 확인한다.
        field_names = ", ".join(data.keys()) if data is not None else ""
        print(f"[성공] {api_name}")
        print(f"  최상위 필드: {field_names}")

    print(f"\n수집 완료: 성공 {success_count}건 / 실패 {len(results) - success_count}건")


# ==================================================
# Step 4. 수집 데이터 검증
# ==================================================
def format_validation_error(error: ValidationError) -> str:
    """Pydantic 검증 오류를 필드명과 원인이 보이도록 정리한다."""
    messages = []
    for detail in error.errors(include_url=False):
        # 중첩 모델의 위치 정보를 hourly.time처럼 읽기 쉬운 필드 경로로 만든다.
        field_name = ".".join(str(part) for part in detail["loc"])
        messages.append(f"{field_name}: {detail['msg']}")
    return "; ".join(messages)


def validate_collected_data(results: list[FetchResult]) -> list[ValidatedResult]:
    """수집된 JSON을 API별 Pydantic 모델로 검증한다."""
    validated_results: list[ValidatedResult] = []

    for api_name, data, collection_error in results:
        # 수집에 실패한 데이터는 Pydantic에 전달하지 않고 실패 원인을 유지한다.
        if collection_error is not None or data is None:
            message = collection_error or "수집된 데이터가 없습니다."
            validated_results.append((api_name, None, f"수집 실패: {message}"))
            continue

        # API 이름을 기준으로 Weather·Country·IP 모델 중 하나를 선택한다.
        model = MODEL_BY_API[api_name]
        try:
            # model_validate()가 필요한 필드를 추출하고 타입·범위 조건을 검사한다.
            validated_data = model.model_validate(data)
            validated_results.append((api_name, validated_data, None))
        except ValidationError as error:
            # 하나의 API가 검증에 실패해도 나머지 API 검증은 계속 수행한다.
            validated_results.append(
                (api_name, None, format_validation_error(error))
            )

    return validated_results


def print_validation_summary(results: list[ValidatedResult]) -> None:
    """API별 스키마 검증 결과를 출력한다."""
    success_count = 0

    for api_name, validated_data, error in results:
        # 실패한 API는 정리된 Pydantic 오류를 출력한다.
        if error is not None:
            print(f"[검증 실패] {api_name}: {error}")
            continue

        success_count += 1
        # 실제로 적용된 Pydantic 모델명을 출력해 API별 검증 경로를 확인한다.
        model_name = type(validated_data).__name__
        print(f"[검증 성공] {api_name}: {model_name}")

    print(f"\n검증 완료: 성공 {success_count}건 / 실패 {len(results) - success_count}건")


# ==================================================
# Step 5. DataFrame 변환·저장·성능 비교
# ==================================================
def normalize_validated_data(
    results: list[ValidatedResult],
) -> dict[str, pd.DataFrame]:
    """검증에 성공한 데이터를 API별 DataFrame으로 변환한다."""
    datasets: dict[str, pd.DataFrame] = {}

    for _, validated_data, error in results:
        # 검증에 실패했거나 데이터가 없는 API는 저장 대상에서 제외한다.
        if error is not None or validated_data is None:
            continue

        if isinstance(validated_data, WeatherResponse):
            hourly = validated_data.hourly
            # 동일한 인덱스의 시간·기온·강수확률을 72개의 시간별 행으로 변환한다.
            # 위치와 시간대는 모든 시간별 행에서 공통으로 사용하는 기준 정보다.
            datasets["weather"] = pd.DataFrame(
                {
                    "time": pd.to_datetime(hourly.time),
                    "temperature_2m": hourly.temperature_2m,
                    "precipitation_probability": hourly.precipitation_probability,
                    "latitude": validated_data.latitude,
                    "longitude": validated_data.longitude,
                    "timezone": validated_data.timezone,
                }
            )
        elif isinstance(validated_data, CountryResponse):
            # 통화·언어처럼 여러 값을 가진 필드는 CSV 한 셀에 저장할 수 있도록 합친다.
            datasets["country"] = pd.DataFrame(
                [
                    {
                        "name": validated_data.name,
                        "alpha2_code": validated_data.alpha_2_code,
                        "alpha3_code": validated_data.alpha_3_code,
                        "capital": validated_data.capital,
                        "region": validated_data.region,
                        "population": validated_data.population,
                        "area": validated_data.area,
                        "currencies": ", ".join(
                            currency.code for currency in validated_data.currencies
                        ),
                        "languages": ", ".join(
                            language.name for language in validated_data.languages
                        ),
                    }
                ]
            )
        elif isinstance(validated_data, IpLocationResponse):
            # IPvAnyAddress 객체는 CSV·Parquet에서 공통으로 저장할 수 있게 문자열로 변환한다.
            datasets["ip_location"] = pd.DataFrame(
                [
                    {
                        "ip": str(validated_data.query),
                        "country": validated_data.country,
                        "country_code": validated_data.country_code,
                        "region": validated_data.region_name,
                        "city": validated_data.city,
                        "latitude": validated_data.latitude,
                        "longitude": validated_data.longitude,
                        "timezone": validated_data.timezone,
                    }
                ]
            )

    return datasets


def remove_generated_files(data_directory: Path = Path("data")) -> None:
    """이 프로그램이 생성하는 CSV·Parquet 파일만 제거한다."""
    # 사용자 파일을 보호하기 위해 예상한 파일명 8개만 정확히 삭제 대상으로 삼는다.
    for dataset_name in GENERATED_DATASETS:
        for file_format in ("csv", "parquet"):
            path = data_directory / f"{dataset_name}.{file_format}"
            path.unlink(missing_ok=True)


def ensure_complete_datasets(
    datasets: dict[str, pd.DataFrame],
    data_directory: Path = Path("data"),
) -> None:
    """세 API 데이터가 모두 준비된 경우에만 저장을 허용한다."""
    # 일부 API만 성공한 상태에서 과거 결과와 새 결과가 섞이지 않도록 완전성을 확인한다.
    missing_datasets = EXPECTED_DATASETS.difference(datasets)
    if missing_datasets:
        # 누락이 있으면 이전 실행에서 만든 결과를 제거하고 저장 단계로 진행하지 않는다.
        remove_generated_files(data_directory)
        missing_names = ", ".join(sorted(missing_datasets))
        raise RuntimeError(
            f"필수 데이터셋 누락으로 저장을 중단합니다: {missing_names}"
        )


def benchmark_storage(
    datasets: dict[str, pd.DataFrame],
    data_directory: Path = Path("data"),
) -> pd.DataFrame:
    """CSV·Parquet 저장과 로딩 시간 및 전체 파일 크기를 측정한다."""
    if not datasets:
        raise ValueError("저장할 검증 데이터가 없습니다.")

    # GitHub에서 내려받아 data 폴더가 없더라도 실행 시 자동으로 생성한다.
    data_directory.mkdir(parents=True, exist_ok=True)
    measurements: list[dict[str, float | int | str]] = []
    # 전달받은 모든 DataFrame의 행 수를 합산해 데이터 규모를 결과에 함께 기록한다.
    total_rows = sum(len(dataframe) for dataframe in datasets.values())

    for file_format in ("csv", "parquet"):
        # perf_counter()는 짧은 실행 시간을 측정하기 위한 고해상도 타이머다.
        write_started = perf_counter()
        paths: list[Path] = []

        for dataset_name, dataframe in datasets.items():
            path = data_directory / f"{dataset_name}.{file_format}"
            if file_format == "csv":
                # index=False로 DataFrame의 자동 인덱스 열이 파일에 저장되지 않게 한다.
                dataframe.to_csv(path, index=False)
            else:
                # PyArrow 엔진을 통해 컬럼 기반 Parquet 파일로 저장한다.
                dataframe.to_parquet(path, index=False)
            paths.append(path)

        write_seconds = perf_counter() - write_started

        # 방금 저장한 모든 파일을 다시 읽어 형식별 전체 로딩 시간을 측정한다.
        read_started = perf_counter()
        for path in paths:
            if file_format == "csv":
                pd.read_csv(path)
            else:
                pd.read_parquet(path)
        read_seconds = perf_counter() - read_started

        measurements.append(
            {
                "rows": total_rows,
                "format": file_format.upper(),
                "write_seconds": write_seconds,
                "read_seconds": read_seconds,
                # 전달받은 데이터셋 파일의 크기를 합산해 형식별 공간 사용량을 비교한다.
                "total_bytes": sum(path.stat().st_size for path in paths),
            }
        )

    return pd.DataFrame(measurements)


def print_storage_summary(
    datasets: dict[str, pd.DataFrame],
    measurements: pd.DataFrame,
) -> None:
    """저장된 데이터셋과 CSV·Parquet 성능 비교 결과를 출력한다."""
    dataset_summary = ", ".join(
        f"{name} {len(dataframe)}행" for name, dataframe in datasets.items()
    )
    print(f"저장 완료: {dataset_summary}")
    print("\n원본·대용량 데이터 CSV·Parquet 성능 비교")
    print(measurements.to_string(index=False))


# ==================================================
# Step 6. 전체 파이프라인 실행
# ==================================================
def main() -> None:
    """수집·검증·저장·성능 비교 파이프라인을 실행한다."""
    # Step 3 실행: 이벤트 루프를 시작해 세 API의 JSON을 동시에 수집한다.
    collected_results = asyncio.run(collect_api_data())
    print_collection_summary(collected_results)

    print()
    # Step 4 실행: API별 Pydantic 모델로 수집 결과를 검증한다.
    validated_results = validate_collected_data(collected_results)
    print_validation_summary(validated_results)

    print()
    # Step 5 실행: 검증 데이터를 표로 변환하고 완전성을 확인한다.
    datasets = normalize_validated_data(validated_results)
    ensure_complete_datasets(datasets)
    # 세 데이터셋이 모두 준비된 경우에만 두 형식으로 저장하고 성능을 비교한다.
    small_measurements = benchmark_storage(datasets)
    small_measurements.insert(0, "dataset", "원본")

    # Step 7 실행: 검증된 날씨 데이터를 100,000행으로 확장해 같은 조건으로 측정한다.
    large_weather = create_large_benchmark_dataset(datasets["weather"])
    large_measurements = benchmark_storage({"weather_large": large_weather})
    large_measurements.insert(0, "dataset", "대용량")

    # 두 측정 결과를 세로로 결합해 데이터 규모에 따른 차이를 한 표에서 확인한다.
    comparison = pd.concat(
        [small_measurements, large_measurements],
        ignore_index=True,
    )
    print(f"추가 성능 실험: weather_large {len(large_weather):,}행")
    print_storage_summary(datasets, comparison)


# ==================================================
# Step 7. 추가 기능 - 대용량 데이터 성능 비교
# ==================================================
def create_large_benchmark_dataset(
    weather_data: pd.DataFrame,
    target_rows: int = 100_000,
) -> pd.DataFrame:
    """검증된 날씨 데이터를 확장해 성능 비교용 DataFrame을 만든다."""
    if weather_data.empty:
        raise ValueError("확장할 날씨 데이터가 없습니다.")
    if target_rows <= 0:
        raise ValueError("목표 행 수는 1 이상이어야 합니다.")

    # 목표 행 수를 채우는 데 필요한 원본 DataFrame 반복 횟수를 올림 계산한다.
    repeat_count = (target_rows + len(weather_data) - 1) // len(weather_data)

    # 검증된 72행을 반복한 뒤 iloc으로 정확히 target_rows만 선택한다.
    # 이 데이터는 API 추가 호출 없이 저장 형식의 규모별 성능을 확인하기 위한 실험용이다.
    large_data = pd.concat(
        [weather_data] * repeat_count,
        ignore_index=True,
    ).iloc[:target_rows].copy()

    # 반복된 각 행을 구분할 수 있도록 1부터 시작하는 고유 번호를 추가한다.
    large_data.insert(0, "record_id", range(1, target_rows + 1))
    return large_data


if __name__ == "__main__":
    # 이 파일을 직접 실행할 때만 전체 파이프라인을 시작한다.
    # 테스트에서 import할 때는 API 호출과 파일 저장이 자동 실행되지 않는다.
    main()
