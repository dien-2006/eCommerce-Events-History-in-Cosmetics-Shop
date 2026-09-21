"""Validated project configuration and SQL boundary helpers."""

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
SHA256 = re.compile(r"^[a-f0-9]{64}$")
RAW_COLUMNS = (
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)
OPTIONAL_COLUMNS = ("payment_method",)


def identifier(value: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL/project identifier: {value!r}")
    return value


def digest(value: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise ValueError("Expected a SHA-256 hex digest")
    return value


def sql_string(value: str) -> str:
    # Spark treats backslashes as escapes even in SQL string literals.
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def as_of_date(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("as_of_date must be YYYY-MM-DD")
    return value


def validate_header(header: list[str]) -> list[str]:
    if len(header) != len(set(header)):
        raise ValueError("CSV contains duplicate column names")
    missing = set(RAW_COLUMNS) - set(header)
    unknown = set(header) - set(RAW_COLUMNS + OPTIONAL_COLUMNS)
    if missing or unknown:
        raise ValueError(
            f"CSV contract mismatch: missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    return header


@dataclass(frozen=True)
class Project:
    name: str
    owner: str
    schedule: str | None
    input_dir: Path
    dbt_dir: Path
    max_invalid_ratio: float
    description: str = ""

    @property
    def bronze(self):
        return f"{self.name}_bronze"

    @property
    def silver(self):
        return f"{self.name}_silver"

    @property
    def gold(self):
        return f"{self.name}_gold"

    @property
    def serving(self):
        return f"{self.name}_analytics"


def load_projects(path: str | None = None) -> dict[str, Project]:
    config_path = path or os.environ.get("PROJECT_CONFIG", "config/projects.yml")
    with open(config_path) as stream:
        config = yaml.safe_load(stream)
    projects = {}
    for name, spec in config["projects"].items():
        identifier(name)
        ratio = float(spec.get("max_invalid_ratio", 0.01))
        if not 0 <= ratio <= 1:
            raise ValueError("max_invalid_ratio must be between 0 and 1")
        projects[name] = Project(
            name=name,
            owner=spec["owner"],
            schedule=spec.get("schedule"),
            input_dir=Path(spec["input_dir"]),
            dbt_dir=Path(spec["dbt_dir"]),
            max_invalid_ratio=ratio,
            description=spec.get("description", ""),
        )
    if not projects:
        raise ValueError("Configure at least one project")
    return projects
