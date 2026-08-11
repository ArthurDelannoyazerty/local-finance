from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    database_path: Path
    frontend_dist: Path

    @classmethod
    def from_environment(cls) -> Settings:
        repository_root = Path(__file__).resolve().parents[2]
        data_dir = Path(os.getenv("LOCAL_FINANCE_DATA_DIR", repository_root / "data"))
        database_path = Path(os.getenv("LOCAL_FINANCE_DB_PATH", data_dir / "finance.db"))
        frontend_dist = Path(
            os.getenv(
                "LOCAL_FINANCE_FRONTEND_DIST",
                repository_root / "frontend" / "dist",
            )
        )
        return cls(
            data_dir=data_dir,
            database_path=database_path,
            frontend_dist=frontend_dist,
        )


settings = Settings.from_environment()
