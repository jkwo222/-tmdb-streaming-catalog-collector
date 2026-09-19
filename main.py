import os
import asyncio
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="TMDB Streaming Catalog Collector", version="1.0")

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_TOKEN = os.environ.get("TMDB_READ_TOKEN")

# Provider IDs will ultimately be refreshed from TMDB's provider-list
# endpoints rather than assumed permanently.
DEFAULT_REGION = "US"


def headers():
    if not TMDB_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="TMDB_READ_TOKEN is not configured",
        )
    return {
        "Authorization": f"Bearer {TMDB_TOKEN}",
        "accept": "application/json",
    }


async def tmdb_get(path: str, params: dict | None = None):
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{TMDB_BASE}{path}",
            headers=headers(),
            params=params or {},
        )
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.text[:1000],
            )
        return response.json()


@app.get("/")
async def root():
    return {
        "service": "tmdb-streaming-catalog-collector",
        "status": "ok",
        "tmdb_token_configured": bool(TMDB_TOKEN),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/providers/{media_type}")
async def providers(
    media_type: Literal["movie", "tv"],
    region: str = DEFAULT_REGION,
):
    data = await tmdb_get(f"/watch/providers/{media_type}")
    providers = [
        p for p in data.get("results", [])
        if region in p.get("display_priorities", {})
    ]
    return {
        "region": region,
        "media_type": media_type,
        "count": len(providers),
        "providers": providers,
    }


@app.get("/discover/{media_type}")
async def discover(
    media_type: Literal["movie", "tv"],
    provider_id: int,
    page: int = Query(1, ge=1, le=500),
    region: str = DEFAULT_REGION,
    monetization: str = "flatrate",
    sort_by: str = "popularity.desc",
):
    params = {
        "watch_region": region,
        "with_watch_providers": provider_id,
        "with_watch_monetization_types": monetization,
        "sort_by": sort_by,
        "page": page,
        "include_adult": "false",
    }

    data = await tmdb_get(f"/discover/{media_type}", params=params)

    return {
        "media_type": media_type,
        "provider_id": provider_id,
        "region": region,
        "monetization": monetization,
        "page": data.get("page"),
        "total_pages": data.get("total_pages"),
        "total_results": data.get("total_results"),
        "results": data.get("results", []),
    }


@app.get("/title/{media_type}/{tmdb_id}")
async def title_details(
    media_type: Literal["movie", "tv"],
    tmdb_id: int,
):
    # append_to_response reduces API calls and gives us identity,
    # credits and provider evidence in one request.
    return await tmdb_get(
        f"/{media_type}/{tmdb_id}",
        params={
            "append_to_response":
                "external_ids,credits,watch/providers,keywords"
        },
    )


@app.get("/collect/{media_type}")
async def collect(
    media_type: Literal["movie", "tv"],
    provider_id: int,
    region: str = DEFAULT_REGION,
    max_pages: int = Query(50, ge=1, le=500),
):
    first = await tmdb_get(
        f"/discover/{media_type}",
        params={
            "watch_region": region,
            "with_watch_providers": provider_id,
            "with_watch_monetization_types": "flatrate",
            "sort_by": "popularity.desc",
            "page": 1,
            "include_adult": "false",
        },
    )

    total_pages = min(first.get("total_pages", 1), max_pages)
    results = list(first.get("results", []))

    semaphore = asyncio.Semaphore(8)

    async def fetch_page(page: int):
        async with semaphore:
            return await tmdb_get(
                f"/discover/{media_type}",
                params={
                    "watch_region": region,
                    "with_watch_providers": provider_id,
                    "with_watch_monetization_types": "flatrate",
                    "sort_by": "popularity.desc",
                    "page": page,
                    "include_adult": "false",
                },
            )

    if total_pages > 1:
        pages = await asyncio.gather(
            *(fetch_page(p) for p in range(2, total_pages + 1))
        )
        for page in pages:
            results.extend(page.get("results", []))

    # Deduplicate defensively by TMDB ID.
    unique = {}
    for item in results:
        if item.get("id") is not None:
            unique[item["id"]] = item

    return {
        "media_type": media_type,
        "provider_id": provider_id,
        "region": region,
        "monetization": "flatrate",
        "reported_total_results": first.get("total_results"),
        "pages_collected": total_pages,
        "unique_titles_collected": len(unique),
        "results": list(unique.values()),
    }
