from __future__ import annotations

import re
import sqlite3
from pathlib import Path


PROFILE_CANDIDATES = (
    "config/profile.yml",
    "config/profile.example.yml",
)


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def normalize_search_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+#/ -]+", " ", str(value or "").lower())).strip()


def parse_scalar(raw: str):
    value = raw.strip()
    if not value:
        return ""
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        return value[1:-1]
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value in {"null", "Null", "~"}:
        return None
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def _strip_comment(line: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    for index, char in enumerate(line):
        if char == "\\" and not escaped:
            escaped = True
            continue
        if char == "'" and not in_double and not escaped:
            in_single = not in_single
        elif char == '"' and not in_single and not escaped:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            return line[:index].rstrip()
        escaped = False
    return line.rstrip()


def _split_inline(value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    for char in value:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if char in "[{":
                depth += 1
            elif char in "]}":
                depth -= 1
            elif char == "," and depth == 0:
                part = "".join(current).strip()
                if part:
                    parts.append(part)
                current = []
                continue
        current.append(char)
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


def _parse_inline_array(value: str) -> list[object]:
    inner = value[1:-1].strip()
    if not inner:
        return []
    return [parse_scalar(part) for part in _split_inline(inner)]


def _parse_inline_object(value: str) -> dict[str, object]:
    inner = value[1:-1].strip()
    if not inner:
        return {}
    result: dict[str, object] = {}
    for part in _split_inline(inner):
        key, sep, rest = part.partition(":")
        if sep:
            result[key.strip()] = parse_scalar(rest)
    return result


def _parse_value(raw: str):
    value = raw.strip()
    if value.startswith("[") and value.endswith("]"):
        return _parse_inline_array(value)
    if value.startswith("{") and value.endswith("}"):
        return _parse_inline_object(value)
    return parse_scalar(value)


def load_yaml_like(path: Path):
    raw_lines = path.read_text(encoding="utf-8").replace("\r\n", "\n").split("\n")
    lines = [_strip_comment(line) for line in raw_lines]
    index = 0

    def indent_of(line: str) -> int:
        return len(line) - len(line.lstrip(" "))

    def skip_noise() -> None:
        nonlocal index
        while index < len(lines) and not lines[index].strip():
            index += 1

    def parse_block(expected_indent: int):
        nonlocal index
        skip_noise()
        if index >= len(lines):
            return {}
        current = lines[index]
        if indent_of(current) < expected_indent:
            return {}
        if current.lstrip().startswith("- "):
            return parse_list(expected_indent)
        return parse_map(expected_indent)

    def parse_list(expected_indent: int) -> list[object]:
        nonlocal index
        result: list[object] = []
        while index < len(lines):
            skip_noise()
            if index >= len(lines):
                break
            line = lines[index]
            indent = indent_of(line)
            stripped = line.strip()
            if indent < expected_indent or not stripped.startswith("- "):
                break

            payload = stripped[2:].strip()
            index += 1
            if not payload:
                result.append(parse_block(expected_indent + 2))
                continue
            if ":" in payload:
                key, _, rest = payload.partition(":")
                item: dict[str, object] = {key.strip(): _parse_value(rest)}
                while True:
                    skip_noise()
                    if index >= len(lines):
                        break
                    nested = lines[index]
                    nested_indent = indent_of(nested)
                    nested_stripped = nested.strip()
                    if nested_indent <= indent:
                        break
                    if nested_stripped.startswith("- "):
                        break
                    nested_key, _, nested_rest = nested_stripped.partition(":")
                    index += 1
                    if nested_rest.strip():
                        item[nested_key.strip()] = _parse_value(nested_rest)
                    else:
                        item[nested_key.strip()] = parse_block(nested_indent + 2)
                result.append(item)
                continue
            result.append(_parse_value(payload))
        return result

    def parse_map(expected_indent: int) -> dict[str, object]:
        nonlocal index
        result: dict[str, object] = {}
        while index < len(lines):
            skip_noise()
            if index >= len(lines):
                break
            line = lines[index]
            indent = indent_of(line)
            stripped = line.strip()
            if indent < expected_indent or stripped.startswith("- "):
                break
            key, _, rest = stripped.partition(":")
            index += 1
            if rest.strip():
                result[key.strip()] = _parse_value(rest)
            else:
                result[key.strip()] = parse_block(indent + 2)
        return result

    return parse_block(0)


def display_label(name: str) -> str:
    raw = str(name or "").strip()
    key = normalize_key(raw)
    special = {
        "cpp": "C++",
        "vr": "VR / XR",
        "xr": "VR / XR",
        "genai": "GenAI",
        "devops": "DevOps",
        "gamebackend": "Game Backend",
        "gameserver": "Game Server",
        "fullstack": "Full Stack",
        "liveops": "Live-Ops",
    }
    return special.get(key, raw.replace("-", " ").title())


def fit_label_to_modifier(label: object) -> float:
    mapping = {
        "primary": 1.0,
        "secondary": 0.5,
        "tertiary": 0.0,
        "adjacent": -0.5,
        "curious": -1.5,
    }
    normalized = normalize_key(str(label or ""))
    return mapping.get(normalized, -0.5)


def resolve_profile_path(repo_root: Path) -> Path | None:
    for relative in PROFILE_CANDIDATES:
        candidate = repo_root / relative
        if candidate.exists():
            return candidate
    return None


def extract_profile_archetypes(profile_path: Path) -> list[dict[str, object]]:
    payload = load_yaml_like(profile_path)
    target_roles = payload.get("target_roles") or {}
    rows = target_roles.get("archetypes") or []
    if isinstance(rows, list) and rows:
        result = []
        for row in rows:
            if not isinstance(row, dict):
                name = str(row or "").strip()
                if name:
                    result.append({"name": name})
                continue
            name = str(row.get("name") or "").strip()
            if name:
                result.append(dict(row))
        if result:
            return result
    primary = target_roles.get("primary") or []
    result = []
    for row in primary if isinstance(primary, list) else []:
        name = str(row or "").strip()
        if name:
            result.append({"name": name, "fit": "primary", "interest": 8, "notes": ""})
    return result


def load_dynamic_archetype_catalog(repo_root: Path, db_path: Path | None = None) -> dict[str, object]:
    ordered_keys: list[str] = []
    labels: dict[str, str] = {}
    fit_modifiers: dict[str, float] = {}

    def remember(key: str, label: str | None = None, fit_modifier: float | None = None) -> None:
        normalized = normalize_key(key)
        if not normalized:
            return
        if normalized not in ordered_keys:
            ordered_keys.append(normalized)
        if label:
            labels[normalized] = label
        else:
            labels.setdefault(normalized, display_label(normalized))
        if fit_modifier is not None:
            fit_modifiers[normalized] = fit_modifier

    profile_path = resolve_profile_path(repo_root)
    if profile_path:
        for row in extract_profile_archetypes(profile_path):
            key = str(row.get("name") or "").strip()
            if not key:
                continue
            remember(key, display_label(key), fit_label_to_modifier(row.get("fit")))

    resolved_db_path = db_path or (repo_root / "data" / "job-log.db")
    if resolved_db_path.exists():
        try:
            with sqlite3.connect(resolved_db_path) as connection:
                connection.row_factory = sqlite3.Row
                tables = {
                    row["name"]
                    for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
                }
                if "archetype_resume_configs" in tables:
                    for row in connection.execute(
                        "SELECT archetype_key, label FROM archetype_resume_configs WHERE approved = 1 ORDER BY archetype_key"
                    ).fetchall():
                        remember(row["archetype_key"], row["label"])
                if "resume_profiles" in tables:
                    for row in connection.execute(
                        "SELECT profile_key FROM resume_profiles WHERE approved = 1 ORDER BY profile_key"
                    ).fetchall():
                        remember(row["profile_key"])
                if "job_archetype_rules" in tables:
                    for row in connection.execute(
                        "SELECT DISTINCT archetype_key FROM job_archetype_rules WHERE approved = 1 ORDER BY archetype_key"
                    ).fetchall():
                        remember(row["archetype_key"])
        except sqlite3.Error:
            pass

    if not ordered_keys:
        remember("general", "General")

    return {
        "keys": ordered_keys,
        "labels": labels,
        "fit_modifiers": fit_modifiers,
    }
