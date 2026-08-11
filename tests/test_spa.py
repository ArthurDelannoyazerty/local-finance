from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from local_finance.main import SPAStaticFiles


def test_spa_routes_fall_back_to_index_without_masking_api_404(tmp_path) -> None:
    (tmp_path / "index.html").write_text("<title>Local Finance</title>", encoding="utf-8")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "app-hash.js").write_text("export {};", encoding="utf-8")
    app = FastAPI()
    app.mount("/", SPAStaticFiles(directory=tmp_path, html=True))

    with TestClient(app) as client:
        deep_link = client.get("/portfolio")
        assert deep_link.status_code == 200
        assert "Local Finance" in deep_link.text
        assert client.get("/api/missing").status_code == 404
        asset = client.get("/assets/app-hash.js")
        assert asset.headers["cache-control"] == "public, max-age=31536000, immutable"
