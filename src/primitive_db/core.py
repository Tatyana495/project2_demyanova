"""Основная логика работы с таблицами и данными."""

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from .decorators import confirm_action, handle_db_errors, log_time

ID_NAME: str = "ID"
SUPPORTED_TYPES = ("int", "str", "bool")
Column = Tuple[str, Union[str, type]]
Row = Dict[str, Any]
TableData = List[Row]


def create_cacher() -> Callable[[Any, Callable[[], Any]], Any]:
    cache: Dict[Any, Any] = {}

    def cache_result(key: Any, value_func: Callable[[], Any]) -> Any:
        if key in cache:
            return cache[key]
        val = value_func()
        cache[key] = val
        return val

    return cache_result


_select_cache = create_cacher()
_select_cache_version: int = 0


def _bump_select_cache() -> None:
    global _select_cache_version
    _select_cache_version += 1


def _select_cache_key(
    data: List[Dict[str, Any]],
    where: Optional[Dict[str, Any]],
) -> Tuple[Any, ...]:
    wc = tuple(sorted(where.items())) if where else ()
    keys = [k for k, _ in wc]
    ids_fp = tuple(sorted(row.get(ID_NAME, 0) for row in data))
    fields_fp = tuple(
        (k, tuple(row.get(k) for row in data))
        for k in sorted(keys)
    )
    return (
        "select",
        _select_cache_version,
        id(data),
        len(data),
        ids_fp,
        wc,
        fields_fp,
    )


def _normalize_declared_type(t: Any) -> str:
    aliases = {
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
    key = t.strip().lower() if isinstance(t, str) else t
    out = aliases.get(key)
    if out is None:
        raise ValueError(
            "Неподдерживаемый тип колонки: {!r}. Допустимы: {}."
            .format(t, SUPPORTED_TYPES)
        )
    return out


def _is_value_of_type(value: Any, typ: str) -> bool:
    if typ == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if typ == "str":
        return isinstance(value, str)
    if typ == "bool":
        return isinstance(value, bool)
    return False


def _matches(row: Dict[str, Any], where: Optional[Dict[str, Any]]) -> bool:
    if not where:
        return True
    for k, v in where.items():
        if row.get(k) != v:
            return False
    return True


@handle_db_errors(fallback=lambda metadata, *a, **k: metadata)
def create_table(
    metadata: Dict[str, Any],
    table_name: str,
    columns: Iterable[Column],
) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")

    tables = metadata.setdefault("tables", {})

    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    if table_name in tables:
        raise ValueError("Таблица '{}' уже существует."
                         .format(table_name))

    normalized: List[Tuple[str, str]] = []
    for col in columns:
        if not (isinstance(col, (tuple, list)) and len(col) == 2):
            raise ValueError("Столбец должен быть кортежем (name, type).")
        name, typ = col
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Имя столбца должно быть непустой строкой.")
        norm_type = _normalize_declared_type(typ)
        normalized.append((name.strip(), norm_type))

    all_names = [ID_NAME] + [n for n, _ in normalized]
    if len(set(n.lower() for n in all_names)) != len(all_names):
        raise ValueError("Найдены дубликаты имён столбцов.")

    cols_dict: Dict[str, str] = {ID_NAME: "int"}
    cols_dict.update({name: typ for name, typ in normalized})

    tables[table_name] = {
        "columns": cols_dict,
        "auto_increment": 1,
        "rows": 0,
        "data": [],
    }
    return metadata


@confirm_action("удаление таблицы")
@handle_db_errors(fallback=lambda metadata, *a, **k: metadata)
def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")
    tables = metadata.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("В metadata отсутствует раздел 'tables'.")
    if table_name not in tables:
        raise ValueError("Таблица '{}' не существует."
                         .format(table_name))
    del tables[table_name]
    return metadata


def _insert_fallback(
    metadata: Dict[str, Any],
    table_name: str,
    *a: Any,
    **k: Any,
) -> List[Dict[str, Any]]:
    try:
        return metadata.get("tables", {}).get(table_name, {}).get("data", [])
    except Exception:
        return []


@log_time
@handle_db_errors(fallback=_insert_fallback)
def insert(
    metadata: Dict[str, Any],
    table_name: str,
    values: Iterable[Any],
) -> List[Dict[str, Any]]:
    if not isinstance(metadata, dict):
        raise ValueError("metadata должен быть словарём.")
    if not isinstance(table_name, str) or not table_name.strip():
        raise ValueError("Некорректное имя таблицы.")

    tables = metadata.get("tables")
    if not isinstance(tables, dict) or table_name not in tables:
        raise ValueError("Таблица '{}' не существует."
                         .format(table_name))

    table = tables[table_name]
    columns = table.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise ValueError("У таблицы '{}' не определены столбцы."
                         .format(table_name))

    user_cols = [col for col in columns.keys() if col != ID_NAME]
    decl_types: Dict[str, str] = {
        col_name: _normalize_declared_type(col_type)
        for col_name, col_type in columns.items()
    }

    values = list(values)
    if len(values) != len(user_cols):
        raise ValueError(
            "Неверное число значений: ожидалось {}, получено: {}."
            .format(len(user_cols), len(values))
        )

    for col_name, value in zip(user_cols, values):
        need_type = decl_types[col_name]
        if not _is_value_of_type(value, need_type):
            raise ValueError(
                "Неверный тип для '{}': ожидался {}, получен {}."
                .format(col_name, need_type, type(value).__name__)
            )

    data = table.setdefault("data", [])
    if not isinstance(data, list):
        raise ValueError("Ожидался список данных в таблице '{}'."
                         .format(table_name))

    auto_inc = table.get("auto_increment")
    if not isinstance(auto_inc, int) or auto_inc < 1:
        auto_inc = len(data) + 1

    new_row: Dict[str, Any] = {ID_NAME: auto_inc}
    for col_name, value in zip(user_cols, values):
        new_row[col_name] = value
    data.append(new_row)

    table["auto_increment"] = auto_inc + 1
    table["rows"] = int(table.get("rows", 0)) + 1
    _bump_select_cache()
    return data


@log_time
@handle_db_errors(fallback=lambda table_data, *a, **k: [])
def select(
    table_data: TableData,
    where_clause: Optional[Dict[str, Any]] = None,
) -> TableData:
    if not isinstance(table_data, list):
        raise ValueError("table_data должен быть списком словарей.")

    key = _select_cache_key(table_data, where_clause)

    def _compute() -> TableData:
        return [r for r in table_data if _matches(r, where_clause)]

    return _select_cache(key, _compute)


@handle_db_errors(fallback=lambda table_data, *a, **k: table_data)
def update(
    table_data: TableData,
    set_clause: Dict[str, Any],
    where_clause: Optional[Dict[str, Any]] = None,
) -> TableData:
    if not isinstance(table_data, list):
        raise ValueError("table_data должен быть списком словарей.")
    if not isinstance(set_clause, dict) or not set_clause:
        raise ValueError("set_clause должен быть непустым словарём.")

    for row in table_data:
        if _matches(row, where_clause):
            for k, v in set_clause.items():
                row[k] = v

    _bump_select_cache()
    return table_data


@confirm_action("удаление строк")
@handle_db_errors(fallback=lambda table_data, *a, **k: table_data)
def delete(
    table_data: TableData,
    where_clause: Optional[Dict[str, Any]] = None,
) -> TableData:
    keep: List[Row] = [
        row for row in table_data if not _matches(row, where_clause)
    ]
    table_data[:] = keep
    _bump_select_cache()
    return table_data
