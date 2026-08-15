from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.gzip import GZipMiddleware

from . import __version__
from .api import router
from .db import database
from .settings import settings


class SPAStaticFiles(StaticFiles):
    """Serve the React entry point for client routes, but never for /api."""

    def file_response(self, full_path, stat_result, scope, status_code: int = 200):
        response = super().file_response(full_path, stat_result, scope, status_code)
        cache_policy = (
            "public, max-age=31536000, immutable"
            if scope.get("path", "").startswith("/assets/")
            else "no-cache"
        )
        response.headers.setdefault("Cache-Control", cache_policy)
        return response

    @staticmethod
    def _is_frontend_route(path: str, scope: dict) -> bool:
        return scope.get("method") in {"GET", "HEAD"} and not path.startswith("api/")

    async def get_response(self, path: str, scope: dict):
        try:
            response = await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404 and self._is_frontend_route(path, scope):
                return await super().get_response("index.html", scope)
            raise
        if response.status_code == 404 and self._is_frontend_route(path, scope):
            return await super().get_response("index.html", scope)
        return response


@asynccontextmanager
async def lifespan(_: FastAPI):
    database.initialize()
    yield


app = FastAPI(
    title="Local Finance",
    version=__version__,
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1_000, compresslevel=5)
app.include_router(router)


@app.get("/api", include_in_schema=False)
def api_root() -> dict[str, str]:
    return {"name": "Local Finance", "version": __version__}


if settings.frontend_dist.exists():
    app.mount(
        "/",
        SPAStaticFiles(directory=settings.frontend_dist, html=True),
        name="frontend",
    )
else:

    @app.get("/", include_in_schema=False)
    def development_root() -> JSONResponse:
        return JSONResponse(
            {
                "name": "Local Finance API",
                "message": "Run the Vite frontend development server on port 5173.",
            }
        )


def run() -> None:
    uvicorn.run("local_finance.main:app", host="0.0.0.0", port=8000, reload=False)
