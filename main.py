"""세 개의 공개 API에서 데이터를 비동기로 수집한다."""

import asyncio

import httpx


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
            raise ValueError("최상위 JSON 형식이 객체가 아닙니다.")

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
    except ValueError as error:
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


def print_summary(results: list[FetchResult]) -> None:
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


def main() -> None:
    """비동기 수집 파이프라인을 실행한다."""
    results = asyncio.run(collect_api_data())
    print_summary(results)


if __name__ == "__main__":
    main()
