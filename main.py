"""
프로그램명: Day 1 데이터 수집 미니 파이프라인
설명: 세 공개 API를 비동기로 수집하고 검증·저장·성능 비교를 수행한다.
작성자: 김지성
변경내용:
- Step 1: 제출용 프로젝트 구조를 구성했다.
- Step 2: 가상환경과 필수 패키지 구성을 추가했다.
- Step 3: asyncio와 httpx를 이용한 비동기 API 수집을 구현했다.
- Step 4: Pydantic v2 타입·범위·관계 검증을 구현했다.
- Step 5: 검증 데이터를 CSV·Parquet으로 저장하고 성능을 비교한다.
- Step 6: pytest 테스트와 README 사용 설명을 추가한다.
"""

import asyncio
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

API_ENDPOINTS = {
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

FetchResult = tuple[str, dict[str, object] | None, str | None]
ValidatedResult = tuple[str, BaseModel | None, str | None]

Latitude = Annotated[float, Field(ge=-90, le=90)]
Longitude = Annotated[float, Field(ge=-180, le=180)]
Probability = Annotated[int, Field(ge=0, le=100)]


# Step 4: API별 응답에서 필요한 필드의 타입과 값 범위를 검증한다.
class ApiModel(BaseModel):
    """공개 API 응답 모델의 공통 설정이다."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class HourlyWeather(ApiModel):
    """Open-Meteo의 시간별 날씨 데이터이다."""

    time: list[str] = Field(min_length=1)
    temperature_2m: list[float] = Field(min_length=1)
    precipitation_probability: list[Probability] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_array_lengths(self) -> Self:
        """시간·기온·강수확률 배열의 길이가 같은지 검증한다."""
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

    status: Literal["success"]
    country: str = Field(min_length=1)
    country_code: str = Field(alias="countryCode", pattern=r"^[A-Z]{2}$")
    region_name: str = Field(alias="regionName", min_length=1)
    city: str = Field(min_length=1)
    latitude: Latitude = Field(alias="lat")
    longitude: Longitude = Field(alias="lon")
    timezone: str = Field(min_length=1)
    query: IPvAnyAddress


MODEL_BY_API: dict[str, type[BaseModel]] = {
    "Open-Meteo": WeatherResponse,
    "Countries.dev": CountryResponse,
    "ip-api": IpLocationResponse,
}


# Step 3: 공유 비동기 클라이언트로 세 API를 동시에 수집한다.
async def fetch_json(
    client: httpx.AsyncClient,
    api_name: str,
    url: str,
) -> FetchResult:
    """API 하나를 호출하고 이름, JSON 데이터, 오류 메시지를 반환한다."""
    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

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
    timeout = httpx.Timeout(10.0)

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        tasks = [
            fetch_json(client, api_name, url)
            for api_name, url in API_ENDPOINTS.items()
        ]
        return list(await asyncio.gather(*tasks))


def print_collection_summary(results: list[FetchResult]) -> None:
    """수집 성공 여부와 응답의 최상위 필드를 간략하게 출력한다."""
    success_count = 0

    for api_name, data, error in results:
        if error is not None:
            print(f"[실패] {api_name}: {error}")
            continue

        success_count += 1
        field_names = ", ".join(data.keys()) if data is not None else ""
        print(f"[성공] {api_name}")
        print(f"  최상위 필드: {field_names}")

    print(f"\n수집 완료: 성공 {success_count}건 / 실패 {len(results) - success_count}건")


# Step 4: 수집 성공 데이터를 Pydantic 모델로 변환하고 오류를 정리한다.
def format_validation_error(error: ValidationError) -> str:
    """Pydantic 검증 오류를 필드명과 원인이 보이도록 정리한다."""
    messages = []
    for detail in error.errors(include_url=False):
        field_name = ".".join(str(part) for part in detail["loc"])
        messages.append(f"{field_name}: {detail['msg']}")
    return "; ".join(messages)


def validate_collected_data(results: list[FetchResult]) -> list[ValidatedResult]:
    """수집된 JSON을 API별 Pydantic 모델로 검증한다."""
    validated_results: list[ValidatedResult] = []

    for api_name, data, collection_error in results:
        if collection_error is not None or data is None:
            message = collection_error or "수집된 데이터가 없습니다."
            validated_results.append((api_name, None, f"수집 실패: {message}"))
            continue

        model = MODEL_BY_API[api_name]
        try:
            validated_data = model.model_validate(data)
            validated_results.append((api_name, validated_data, None))
        except ValidationError as error:
            validated_results.append(
                (api_name, None, format_validation_error(error))
            )

    return validated_results


def print_validation_summary(results: list[ValidatedResult]) -> None:
    """API별 스키마 검증 결과를 출력한다."""
    success_count = 0

    for api_name, validated_data, error in results:
        if error is not None:
            print(f"[검증 실패] {api_name}: {error}")
            continue

        success_count += 1
        model_name = type(validated_data).__name__
        print(f"[검증 성공] {api_name}: {model_name}")

    print(f"\n검증 완료: 성공 {success_count}건 / 실패 {len(results) - success_count}건")


# Step 5: 검증된 모델을 API별 표 형태로 정규화한다.
def normalize_validated_data(
    results: list[ValidatedResult],
) -> dict[str, pd.DataFrame]:
    """검증에 성공한 데이터를 API별 DataFrame으로 변환한다."""
    datasets: dict[str, pd.DataFrame] = {}

    for _, validated_data, error in results:
        if error is not None or validated_data is None:
            continue

        if isinstance(validated_data, WeatherResponse):
            hourly = validated_data.hourly
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


def benchmark_storage(
    datasets: dict[str, pd.DataFrame],
    data_directory: Path = Path("data"),
) -> pd.DataFrame:
    """CSV·Parquet 저장과 로딩 시간 및 전체 파일 크기를 측정한다."""
    if not datasets:
        raise ValueError("저장할 검증 데이터가 없습니다.")

    data_directory.mkdir(parents=True, exist_ok=True)
    measurements: list[dict[str, float | int | str]] = []

    for file_format in ("csv", "parquet"):
        write_started = perf_counter()
        paths: list[Path] = []

        for dataset_name, dataframe in datasets.items():
            path = data_directory / f"{dataset_name}.{file_format}"
            if file_format == "csv":
                dataframe.to_csv(path, index=False)
            else:
                dataframe.to_parquet(path, index=False)
            paths.append(path)

        write_seconds = perf_counter() - write_started

        read_started = perf_counter()
        for path in paths:
            if file_format == "csv":
                pd.read_csv(path)
            else:
                pd.read_parquet(path)
        read_seconds = perf_counter() - read_started

        measurements.append(
            {
                "format": file_format.upper(),
                "write_seconds": write_seconds,
                "read_seconds": read_seconds,
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
    print("\nCSV·Parquet 성능 비교")
    print(measurements.to_string(index=False))


def main() -> None:
    """수집·검증·저장·성능 비교 파이프라인을 실행한다."""
    collected_results = asyncio.run(collect_api_data())
    print_collection_summary(collected_results)

    print()
    validated_results = validate_collected_data(collected_results)
    print_validation_summary(validated_results)

    print()
    datasets = normalize_validated_data(validated_results)
    measurements = benchmark_storage(datasets)
    print_storage_summary(datasets, measurements)


if __name__ == "__main__":
    main()
