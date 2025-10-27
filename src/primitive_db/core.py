"""Здесь будет основная логика работы
    с таблицами и данными."""

from typing import Any, Dict, Iterable, List, Tuple, Union

Column = Tuple[str, Union[str, type]]


def create_table(
    metadata: Dict[str, Any],
    table_name: str,
    columns: Iterable[Column],
) -> Dict[str, Any]:
    """
    Создаёт описание таблицы в metadata.

    Параметры
    ---------
    metadata : dict
        Общий словарь метаданных БД. Ожидается структура {"tables": {...}}.
    table_name : str
        Имя новой таблицы.
    columns : iterable[tuple[str, str|type]]
        Список столбцов без ID. Каждый элемент — кортеж (имя, тип),
        где тип — 'int' | 'str' | 'bool' или соответствующий Python-тип
        (int, str, bool).

    Поведение
    ---------
    - Автоматически добавляет столбец ('ID', 'int') в начало.
    - Проверяет, что таблицы с таким именем ещё нет.
    - Валидирует типы и дубли имён столбцов.
    - В случае успеха обновляет metadata и возвращает его.

    Исключения
    ----------
    ValueError — при дублирующемся имени таблицы, неверном формате столбцов,
                 неподдерживаемом типе или дублировании имён колонок.
                 meta = {"tables": {}}

    Пример использования
    --------------------
    meta = create_table(
        meta,
        table_name="users",
        columns=[
            ("name", "str"),
            ("age", int),
            ("is_active", "bool"),
        ],
    )

    print(meta["tables"]["users"]["columns"])
    # {'ID': 'int', 'name': 'str', 'age': 'int', 'is_active': 'bool'}

    """
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")

    tables = metadata.setdefault("tables", {})

    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    if table_name in tables:
        raise ValueError(f"Таблица '{table_name}' уже существует.")

    # Допустимые типы
    allowed_aliases = {
        "int": "int",
        "integer": "int",
        int: "int",
        "str": "str",
        "string": "str",
        str: "str",
        "bool": "bool",
        "boolean": "bool",
        bool: "bool",
    }

    # Нормализуем и валидируем столбцы пользователя
    normalized: List[Tuple[str, str]] = []
    for col in columns:
        if not (isinstance(col, (tuple, list)) and len(col) == 2):
            raise ValueError(
                "Каждый столбец должен быть кортежем (name, type) вида ('age','int') или ('flag', bool)."  # noqa: E501
            )
        name, typ = col
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Имя столбца должно быть непустой строкой.")

        type_key = typ
        if isinstance(typ, str):
            type_key = typ.strip().lower()

        norm_type = allowed_aliases.get(type_key)
        if norm_type is None:
            raise ValueError(
                f"Столбец '{name}': неподдерживаемый тип '{typ}'. "
                "Допустимы только int, str, bool."
            )

        normalized.append((name.strip(), norm_type))

    # Проверка дублей имён, включая зарезервированный ID
    all_names = ["ID"] + [n for n, _ in normalized]
    if len(set(n.lower() for n in all_names)) != len(all_names):
        raise ValueError(
            "Найдены дубликаты имён столбцов."
        )

    # Формируем словарь колонок: ID первым
    cols_dict: Dict[str, str] = {"ID": "int"}
    cols_dict.update({name: typ for name, typ in normalized})

    # Сохраняем описание таблицы
    tables[table_name] = {
        "columns": cols_dict,
        "auto_increment": 1,      # счётчик для ID
        "rows": 0,                # можно вести число строк
    }

    return metadata


def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Удаляет таблицу из метаданных.

    Parameters
    ----------
    metadata : dict
        Общий словарь метаданных БД. Ожидается структура {"tables": {...}}.
    table_name : str
        Имя таблицы для удаления.

    Returns
    -------
    dict
        Обновлённые метаданные.

    Raises
    ------
    ValueError
        Если metadata не словарь, имя таблицы некорректно,
        или таблица не найдена.

    Пример использования
    --------------------
    meta = {"tables": {"users": {"columns": {"ID": "int"},"auto_increment":
    1,"rows": 0}}}
    meta = drop_table(meta, "users")
    print(meta)  # {'tables': {}}

    """
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")

    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    tables = metadata.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("В metadata отсутствует раздел 'tables'.")

    if table_name not in tables:
        raise ValueError(f"Таблица '{table_name}' не существует.")

    # Удаляем описание таблицы
    del tables[table_name]

    return metadata
