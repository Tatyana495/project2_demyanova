"""Для вспомогательных функций
    (например, работа с файлами)."""
from __future__ import annotations

import json
import os

from pathlib import Path
from typing import Any, Dict, List


def _normalize_meta_path(filepath: str) -> Path:
    """
    Возвращает корректный путь к JSON-файлу метаданных.
    - Пустой путь -> storage/metadata.json
    - Каталог или путь без .json -> <path>/metadata.json
    - Иначе -> как есть
    Гарантирует существование родительской папки (кроме '.').
    """
    if not filepath or not filepath.strip():
        p = Path("storage") / "metadata.json"
    else:
        p = Path(filepath)
        if p.suffix.lower() != ".json":
            p = p / "metadata.json"

    parent = p.parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
    return p


def load_metadata(filepath: str) -> Dict[str, Any]:
    """
    Загружает metadata из JSON. При отсутствии файла/битом JSON — {}.
    Папка создаётся автоматически (storage/).
    """
    p = _normalize_meta_path(filepath)
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_metadata(filepath: str, data: Any) -> None:
    """
    Сохраняет metadata в JSON. Папка создаётся автоматически.
    """
    p = _normalize_meta_path(filepath)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# Папка для хранения строк таблиц
DATA_DIR = Path(os.getenv("PRIMITIVE_DB_DATA_DIR", "data"))


def _table_path(table_name: str) -> Path:
    """
    Построить безопасный путь к файлу data/<table_name>.json.
    Автоматически создаёт папку data/ при необходимости.
    """
    safe = table_name.strip().replace("/", "_").replace("\\", "_")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"{safe}.json"


def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    """
    Загрузить строки таблицы из JSON-файла data/<table>.json.
    Если файл отсутствует или повреждён, вернуть пустой список [].
    """
    path = _table_path(table_name)
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []


def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    """
    Сохранить строки таблицы в JSON-файл data/<table>.json.
    Пишем через временный файл, чтобы избежать порчи данных.
    """
    path = _table_path(table_name)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def delete_table_data(table_name: str) -> None:
    """Удаляет файл data/<table>.json, если он существует."""
    path = _table_path(table_name)
    try:
        path.unlink()
    except FileNotFoundError:
        pass
