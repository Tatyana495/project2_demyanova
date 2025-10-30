"""Этот файл отвечает за запуск, игровой цикл и парсинг команд."""
from __future__ import annotations

import os
import re
import shlex
from pathlib import Path

from prettytable import PrettyTable

from .core import create_table, drop_table
from .core import delete as core_delete
from .core import insert as core_insert
from .core import select as core_select
from .core import update as core_update
from .parser import parse_set, parse_value, parse_where
from .utils import (
    delete_table_data,
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


def welcome() -> None:
    """Короткое приветствие без ввода/цикла."""
    print("Первая попытка запустить проект!")
    print("\n***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")


METADATA_FILE = os.getenv("PRIMITIVE_DB_META", "storage/metadata.json")

HELP_TEXT = """
Доступные команды:
  create <table> <col:type> [col:type ...]
  drop <table>
  show tables
  describe <table>
  help
  exit / quit

Операции с данными:
  insert into <table> values (<v1>, <v2>, ...)
  select from <table> [where <col> = <value>]
  update <table> set <col> = <value> [where <col> = <value>]
  delete from <table> where <col> = <value>
  info <table>

Примеры:
  create users name:str age:int is_active:bool
  insert into users values ("Sergei", 28, true)
  select from users where age = 28
""".strip()


def _print_rows_as_table(meta: dict, table: str, rows: list[dict]) -> None:
    """Печатает SELECT с порядком колонок из metadata."""
    tbl_meta = meta["tables"][table]
    cols = tbl_meta.get("columns", {})
    if cols:
        col_order = list(cols.keys())
    else:
        keys: set[str] = set()
        for r in rows:
            keys.update(r.keys())
        col_order = sorted(keys)
        if "ID" in col_order:
            col_order = ["ID"] + [c for c in col_order if c != "ID"]

    pt = PrettyTable()
    pt.field_names = col_order
    for name in col_order:
        pt.align[name] = "c"
    for r in rows:
        pt.add_row([r.get(c, "") for c in col_order])
    print(pt)


def _ensure_tables_dict(meta: dict) -> dict:
    """Гарантирует наличие ключа 'tables' в метаданных."""
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("tables", {})
    return meta


def _print_tables(meta: dict) -> None:
    tables = meta.get("tables", {})
    if not tables:
        print("Таблиц нет.")
        return
    print("Список таблиц:")
    for name in sorted(tables.keys()):
        print(f"  - {name}")


def _describe_table(meta: dict, table: str) -> None:
    tables = meta.get("tables", {})
    info = tables.get(table)
    if not info:
        print(f"Таблица '{table}' не найдена.")
        return
    print(f"Таблица: {table}")
    cols = info.get("columns", {})
    print("Столбцы:")
    for col, typ in cols.items():
        print(f"  {col}: {typ}")
    if "rows" in info:
        print(f"Строк (по учёту): {info['rows']}")


def _parse_columns(tokens: list[str]) -> list[tuple[str, str]]:
    """
    Преобразует список 'name:type' в список кортежей (name, type).
    Допускаются типы: int | str | bool.
    """
    cols: list[tuple[str, str]] = []
    for tok in tokens:
        if ":" not in tok:
            raise ValueError(f"Ожидалось '<col>:<type>', получено: '{tok}'")
        name, typ = tok.split(":", 1)
        name = name.strip()
        typ = typ.strip().lower()
        if typ not in {"int", "str", "bool"}:
            raise ValueError(
                "Неподдерживаемый тип "
                f"'{typ}' (только int|str|bool)"
            )
        cols.append((name, typ))
    return cols


# Разбор и выполнение команд с данными

def _strip_parens(s: str) -> str:
    s = s.strip()
    if not (s.startswith("(") and s.endswith(")")):
        raise ValueError("Ожидались значения в скобках: (...)")
    return s[1:-1].strip()


def _parse_values_list(values_str: str) -> list[object]:
    """
    Разбирает список значений '(<v1>, <v2>, ...)' с учётом кавычек.
    """
    inner = _strip_parens(values_str)
    if inner == "":
        return []
    parts: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch)
        elif ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
        elif ch == "," and not in_single and not in_double:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        parts.append("".join(buf).strip())
    return [parse_value(p) for p in parts]


def _handle_insert(line: str, metadata: dict) -> None:
    
    m = re.search(r"\bvalues\b", line, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Синтаксис: insert into <table> values (...).")

    head = line[:m.start()]
    tail = line[m.end():]

    # head == "insert into <table>"
    lx = shlex.shlex(head, posix=True)
    lx.whitespace_split = True
    toks = [t for t in lx if t.strip()]
    if (
        len(toks) < 3
        or toks[0].lower() != "insert"
        or toks[1].lower() != "into"
    ):
        raise ValueError("Синтаксис: insert into <table> values (...).")
    table = toks[2]

    # 1) грузим текущее содержимое таблицы из data/<table>.json
    data = load_table_data(table)

    # 2) даём core.insert ссылку на тот же список
    tables = metadata.get("tables", {})
    if table not in tables:
        raise ValueError(f"Таблица '{table}' не существует.")
    tables[table]["data"] = data  # одна и та же ссылка

    # 3) парсим и вставляем
    values = _parse_values_list(tail)
    core_insert(metadata, table, values)

    # 4) сохраняем данные таблицы и метаданные
    save_table_data(table, data)
    save_metadata(METADATA_FILE, metadata)

    print(f"OK. Добавлено. Всего записей: {len(data)}")


def _handle_select(line: str, metadata: dict) -> None:
    m = re.match(
        r"^\s*select\s+from\s+"
        r"([A-Za-z_][A-Za-z0-9_]*)"
        r"\s*(?:where\s+(.*))?\s*$",
        line,
        flags=re.IGNORECASE,
    )
    if not m:
        raise ValueError("Синтаксис: select from <table> [where ...].")

    table = m.group(1)
    where_text = m.group(2)
    where_clause = parse_where(where_text) if where_text else None

    tables = metadata.get("tables", {})
    if table not in tables:
        raise ValueError(f"Таблица '{table}' не существует.")

    # 1) читаем из data/<table>.json
    data = load_table_data(table)

    # 2) если файл пуст, но есть данные в metadata — мигрируем их в файл
    meta_data = tables[table].setdefault("data", [])
    if not data and meta_data:
        data = meta_data
        save_table_data(table, data)
        tables[table]["rows"] = len(data)
        save_metadata(METADATA_FILE, metadata)

    # 3) привяжем тот же список к metadata
    tables[table]["data"] = data

    rows = core_select(data, where_clause)
    _print_rows_as_table(metadata, table, rows)


def _handle_update(line: str, metadata: dict) -> None:
    low = line.lower()
    idx_set = low.find(" set ")
    if idx_set == -1:
        raise ValueError(
            "Синтаксис: update <table> set <...> [where <...>]."
        )

    head = line[:idx_set]
    tail_after_set = line[idx_set + 5:]

    # разрезаем хвост на set / where
    low_tail = tail_after_set.lower()
    wpos = low_tail.find(" where ")
    set_text = tail_after_set if wpos == -1 else tail_after_set[:wpos]
    where_text = None if wpos == -1 else tail_after_set[wpos + 7:]

    # имя таблицы
    lx = shlex.shlex(head, posix=True)
    lx.whitespace_split = True
    toks = [t for t in lx if t.strip()]
    if len(toks) < 2 or toks[0].lower() != "update":
        raise ValueError(
            "Синтаксис: update <table> set ... [where ...]."
        )
    table = toks[1]

    set_clause = parse_set(set_text)
    where_clause = (
        parse_where(where_text) if where_text is not None else None
    )

    # грузим, считаем, обновляем, сохраняем
    data = load_table_data(table)
    affected = len(core_select(data, where_clause))
    core_update(data, set_clause, where_clause)
    save_table_data(table, data)

    # обновим счётчик строк и метаданные
    metadata["tables"][table]["rows"] = len(data)
    save_metadata(METADATA_FILE, metadata)

    print(f"OK. Обновлено: {affected} строк(и).")


def _handle_delete(line: str, metadata: dict) -> None:
    # разбор: delete from <table> where ...
    lx = shlex.shlex(line, posix=True)
    lx.whitespace_split = True
    toks = [t for t in lx if t.strip()]
    if (
        len(toks) < 4
        or toks[0].lower() != "delete"
        or toks[1].lower() != "from"
    ):
        raise ValueError(
            "Синтаксис: delete from <table> where <...>."
        )

    table = toks[2]

    # хвост WHERE (регистронезависимо)
    m = re.search(r"\bwhere\b", line, flags=re.IGNORECASE)
    if not m:
        raise ValueError("Для DELETE требуется WHERE.")
    where_text = line[m.end():].strip()
    where_clause = parse_where(where_text)

    # читаем, считаем, удаляем, сохраняем
    data = load_table_data(table)
    affected = len(core_select(data, where_clause))
    core_delete(data, where_clause)
    save_table_data(table, data)

    # обновим rows и метаданные
    metadata["tables"][table]["rows"] = len(data)
    save_metadata(METADATA_FILE, metadata)

    print(f"OK. Удалено: {affected} строк(и).")


def _handle_info(line: str, metadata: dict) -> None:
    lx = shlex.shlex(line, posix=True)
    lx.whitespace_split = True
    toks = [t for t in lx if t.strip()]
    if len(toks) < 2 or toks[0].lower() != "info":
        raise ValueError("Синтаксис: info <table>.")
    table = toks[1]

    tables = metadata.get("tables", {})
    if table not in tables:
        raise ValueError(f"Таблица '{table}' не существует.")
    tbl = tables[table]
    info = {
        "name": table,
        "columns": tbl.get("columns", {}),
        "rows": int(tbl.get("rows", 0)),
        "next_id": int(tbl.get("auto_increment", 1)),
    }
    print(info)


def run() -> None:
    # гарантируем каталоги на старте
    Path("storage").mkdir(parents=True, exist_ok=True)
    Path("data").mkdir(parents=True, exist_ok=True)

    welcome()
    print(HELP_TEXT)

    while True:
        metadata = load_metadata(METADATA_FILE)
        metadata = _ensure_tables_dict(metadata)

        try:
            user_input = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nВыход.")
            break

        if not user_input:
            continue

        try:
            args = shlex.split(user_input)
        except ValueError as e:
            print(f"Ошибка парсинга: {e}")
            continue

        cmd_lower = user_input.strip().split(" ", 1)[0].lower()

        # выход
        if cmd_lower in {"exit", "quit"}:
            print("До встречи!")
            break

        # помощь
        if cmd_lower == "help":
            print(HELP_TEXT)
            continue

        # show tables
        if cmd_lower == "show" and len(args) >= 2 and args[1].lower() == \
                "tables":
            _print_tables(metadata)
            continue

        # describe <table>
        if cmd_lower == "describe":
            if len(args) < 2:
                print("Укажите имя таблицы: describe <table>")
                continue
            _describe_table(metadata, args[1])
            continue

        # create <table> <col:type>...
        if cmd_lower == "create":
            if len(args) < 3:
                print("Синтаксис: create <table> <col:type> [col:type ...]")
                continue
            table_name = args[1]
            try:
                cols = _parse_columns(args[2:])
                metadata = create_table(metadata, table_name, cols)
                save_metadata(METADATA_FILE, metadata)
                # сразу создадим пустой файл данных
                save_table_data(table_name, [])
                print(f"Таблица '{table_name}' создана.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # drop <table>
        if cmd_lower == "drop":
            if len(args) < 2:
                print("Синтаксис: drop <table>")
                continue
            table_name = args[1]
            try:
                metadata = drop_table(metadata, table_name)
                # удалим файл данных таблицы
                delete_table_data(table_name)
                save_metadata(METADATA_FILE, metadata)
                print(f"Таблица '{table_name}' удалена.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # SQL-подобные команды над данными
        try:
            low = user_input.lower()
            if low.startswith("insert"):
                _handle_insert(user_input, metadata)
                continue
            if low.startswith("select"):
                _handle_select(user_input, metadata)
                continue
            if low.startswith("update"):
                _handle_update(user_input, metadata)
                continue
            if low.startswith("delete"):
                _handle_delete(user_input, metadata)
                continue
            if low.startswith("info"):
                _handle_info(user_input, metadata)
                continue
        except Exception as e:
            print(f"Ошибка: {e}")
            continue

        print("Неизвестная команда. Введите 'help'.")
