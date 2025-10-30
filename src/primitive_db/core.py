"""Здесь будет основная логика работы с таблицами и данными."""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union


#  Константы и типы
ID_NAME: str = "ID"
SUPPORTED_TYPES = ("int", "str", "bool")
Column = Tuple[str, Union[str, type]]


# Вспомогательные функции
def _normalize_declared_type(t: Any) -> str:
    """
    Приводит тип из схемы к одному из: 'int' | 'str' | 'bool'.
    Допускает алиасы: 'integer' -> 'int', 'string' -> 'str',
    а также сами Python-типы int/str/bool.
    """
    aliases = {
        "int": "int", "integer": "int", int: "int",
        "str": "str", "string": "str", str: "str",
        "bool": "bool", "boolean": "bool", bool: "bool",
    }
    key = t.strip().lower() if isinstance(t, str) else t
    out = aliases.get(key)
    if out is None:
        raise ValueError(f"Неподдерживаемый тип колонки: {t!r}. Допустимы: {SUPPORTED_TYPES}.")  # noqa: E501
    return out


def _is_value_of_type(value: Any, typ: str) -> bool:
    """Проверка значения на соответствие одному из
        поддерживаемых типов ('int' | 'str' | 'bool')."""
    if typ == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if typ == "str":
        return isinstance(value, str)
    if typ == "bool":
        return isinstance(value, bool)
    return False


def _matches(row: Dict[str, Any], where: Optional[Dict[str, Any]]) -> bool:
    """
    Совпадение строки с условием where (все пары ключ-значение равны).
    Если where == None или пустой словарь — подходит любая строка.
    """
    if not where:
        return True
    for k, v in where.items():
        if row.get(k) != v:
            return False
    return True


# Операции с таблицами

# Создание таблицы
def create_table(
    metadata: Dict[str, Any],
    table_name: str,
    columns: Iterable[Column],
) -> Dict[str, Any]:
    """
    Создаёт описание таблицы в metadata.
    Структура таблицы:
      {
        "columns": {"ID": "int", "name": "str", ...},
        "auto_increment": 1,
        "rows": 0,
        "data": []
      }
    """
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")

    tables = metadata.setdefault("tables", {})

    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    if table_name in tables:
        raise ValueError(f"Таблица '{table_name}' уже существует.")

    # Нормализуем и валидируем столбцы пользователя
    normalized: List[Tuple[str, str]] = []
    for col in columns:
        if not (isinstance(col, (tuple, list)) and len(col) == 2):
            raise ValueError(
                "Каждый столбец должен быть кортежем (name, type) "
                "вида ('age','int') или ('flag', bool)."
            )
        name, typ = col
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Имя столбца должно быть непустой строкой.")

        norm_type = _normalize_declared_type(typ)
        normalized.append((name.strip(), norm_type))

    # Проверка дублей имён, включая зарезервированный ID
    all_names = [ID_NAME] + [n for n, _ in normalized]
    if len(set(n.lower() for n in all_names)) != len(all_names):
        raise ValueError("Найдены дубликаты имён столбцов.")

    # Формируем словарь колонок: ID первым, затем пользовательские колонки
    cols_dict: Dict[str, str] = {ID_NAME: "int"}
    cols_dict.update({name: typ for name, typ in normalized})

    # Сохраняем описание таблицы
    tables[table_name] = {
        "columns": cols_dict,
        "auto_increment": 1,      # счётчик для ID
        "rows": 0,                # число строк
        "data": [],               # хранилище строк
    }

    return metadata


def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """Удаляет таблицу из метаданных."""
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")

    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    tables = metadata.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("В metadata отсутствует раздел 'tables'.")

    if table_name not in tables:
        raise ValueError(f"Таблица '{table_name}' не существует.")

    del tables[table_name]
    return metadata


# Вставка строк
def insert(
    metadata: Dict[str, Any],
    table_name: str,
    values: Iterable[Any],
) -> List[Dict[str, Any]]:
    """
    Добавляет новую запись (dict) в таблицу и возвращает список всех записей.
    Ожидается, что схема таблицы хранится в metadata['tables'][table]['columns'] (dict),  # noqa: E501
    а данные — в metadata['tables'][table]['data'] (list[dict]).
    Значения передаются только для столбцов БЕЗ ID,
    в порядке их объявления при create_table.
    """
    # 1) Проверки аргументов и существования таблицы
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    tables = metadata.get("tables")
    if not isinstance(tables, dict) or table_name not in tables:
        raise ValueError(f"Таблица '{table_name}' не существует.")

    table = tables[table_name]
    columns = table.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise ValueError(f"У таблицы '{table_name}' не определены столбцы.")

    # 2) Список пользовательских столбцов (без ID) в порядке объявления
    user_cols = [col for col in columns.keys() if col != ID_NAME]

    # 3) Нормализуем и валидируем объявленные типы
    decl_types: Dict[str, str] = {col_name: _normalize_declared_type(col_type)
                                  for col_name, col_type in columns.items()}

    # 4) Сравниваем длину values с количеством столбцов без ID
    values = list(values)
    if len(values) != len(user_cols):
        raise ValueError(
            f"Ожидалось значений: {len(user_cols)}, получено: {len(values)}."
        )

    # 5) Валидация типов по схеме
    for col_name, value in zip(user_cols, values):
        need_type = decl_types[col_name]
        if not _is_value_of_type(value, need_type):
            raise TypeError(
                f"Неверный тип для '{col_name}': ожидался {need_type}, получен {type(value).__name__}."  # noqa: E501
            )

    # 6) Подготовка хранилища данных и генерация нового ID
    data = table.setdefault("data", [])
    if not isinstance(data, list):
        raise ValueError(f"Ожидался список данных в таблице '{table_name}'.")

    auto_inc = table.get("auto_increment")
    if not isinstance(auto_inc, int) or auto_inc < 1:
        # если поле испорчено/не задано — восстановим от длины + 1
        auto_inc = len(data) + 1

    new_id = auto_inc  # следующий доступный ID

    # 7) Сбор и добавление новой строки
    new_row: Dict[str, Any] = {ID_NAME: new_id}
    for col_name, value in zip(user_cols, values):
        new_row[col_name] = value

    data.append(new_row)

    # 8) Обновляем служебные поля таблицы
    table["auto_increment"] = new_id + 1
    table["rows"] = int(table.get("rows", 0)) + 1

    return data


# Простые операции над данными таблицы
Row = Dict[str, Any]
TableData = List[Row]


def select(table_data: TableData,
           where_clause: Optional[Dict[str, Any]] = None) -> TableData:
    """
    Если where_clause не задан — возвращает все строки.
    Иначе — только строки, где ВСЕ поля из where_clause равны
    указанным значениям.
    Возвращаются ССЫЛКИ на исходные словари (без копирования).
    """
    if not isinstance(table_data, list):
        raise ValueError("table_data должен быть списком словарей.")
    return [row for row in table_data if _matches(row, where_clause)]


def update(
    table_data: TableData,
    set_clause: Dict[str, Any],
    where_clause: Optional[Dict[str, Any]] = None,
) -> TableData:
    """
    Обновляет поля в найденных записях согласно set_clause.
    Если where_clause пуст — обновляет все строки.
    Возвращает исходный список (изменяется на месте).
    """
    if not isinstance(table_data, list):
        raise ValueError("table_data должен быть списком словарей.")
    if not isinstance(set_clause, dict) or not set_clause:
        raise ValueError("set_clause должен быть непустым словарём.")

    for row in table_data:
        if _matches(row, where_clause):
            for k, v in set_clause.items():
                row[k] = v

    return table_data


def delete(
    table_data: TableData,
    where_clause: Optional[Dict[str, Any]] = None,
) -> TableData:
    keep: List[Row] = [
        row
        for row in table_data
        if not _matches(row, where_clause)
    ]
    table_data[:] = keep
    return table_data
