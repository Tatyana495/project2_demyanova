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
    TypeError, если объект не сериализуем в JSON.
    """
    # гарантируем существование директории
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

    # сохраняем JSON c читабельным форматированием и поддержкой юникода
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
