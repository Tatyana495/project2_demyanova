"""Для вспомогательных функций
    (например, работа с файлами)."""

import json
import os
from typing import Any, Dict


def load_metadata(filepath: str) -> Dict[str, Any]:
    """
    Загружает данные из JSON-файла.
    Если файл не найден — возвращает пустой словарь {}.
    Если файл повреждён (невалидный JSON) — тоже возвращает {}.

    Parameters
    ----------
    filepath : str
        Путь к JSON-файлу.

    Returns
    -------
    dict
        Содержимое JSON или пустой словарь.

    Пример вызова
    -------
    meta = load_metadata("metadata.json")
    print(meta)  # {} если файла нет или JSON битый

    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        # Можно залогировать предупреждение, если есть логгер
        return {}


def save_metadata(filepath: str, data: Any) -> None:
    """
    Сохраняет переданные данные в JSON-файл.
    Создаёт директорию, если её не существует.
    Бросает TypeError, если объект не сериализуем в JSON.

    Parameters
    ----------
    filepath : str
        Путь к JSON-файлу (например, 'data/meta.json').
    data : Any
        Любой JSON-сериализуемый объект
        (dict, list, str, int, float, bool, None).
    Пример использования
    --------------------
    meta = {"version": 1, "tables": ["users", "orders"]}
    save_metadata("storage/metadata.json", meta)
    """
    # гарантируем существование директории
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

    # сохраняем JSON c читабельным форматированием и поддержкой юникода
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
