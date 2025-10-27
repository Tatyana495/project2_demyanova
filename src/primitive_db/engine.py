"""тот файл будет отвечать за запуск,
    игровой цикл и парсинг команд."""

import os
import shlex

from .utils import load_metadata, save_metadata
from .core import create_table, drop_table


def welcome() -> None:
    """Короткое приветствие без ввода/цикла."""
    print("Первая попытка запустить проект!")
    print("\n***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")


METADATA_FILE = os.getenv("PRIMITIVE_DB_META", "storage/metadata.json")  # noqa: E501

HELP_TEXT = """
Доступные команды:
  create <table> <col:type> [col:type ...]  — создать таблицу (типы: int|str|bool)  # noqa: E501
  drop <table>                               — удалить таблицу
  show tables                                — показать список таблиц
  describe <table>                           — показать структуру таблицы
  help                                       — показать подсказку
  exit / quit                                — выход
Примеры:
  create users name:str age:int is_active:bool
  drop users
"""


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


def _parse_columns(tokens):
    """
    Преобразует список 'name:type' в список кортежей (name, type).
    Допускается тип в виде строки ('int','str','bool') или как Python-тип.
    """
    cols = []
    for tok in tokens:
        if ":" not in tok:
            raise ValueError(f"Ожидалось '<col>:<type>', получено: '{tok}'")
        name, typ = tok.split(":", 1)
        name = name.strip()
        typ = typ.strip().lower()
        if typ not in {"int", "str", "bool"}:
            raise ValueError(f"Неподдерживаемый тип '{typ}' (только int|str|bool)")  # noqa: E501
        cols.append((name, typ))
    return cols


def run() -> None:
    welcome()           # ← просто вызываем в начале
    print(HELP_TEXT)

    while True:
        # 1) загружаем актуальные метаданные каждый цикл
        metadata = load_metadata(METADATA_FILE)
        metadata = _ensure_tables_dict(metadata)

        try:
            user_input = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nВыход.")
            break

        if not user_input:
            continue

        # разбор ввода безопасно
        try:
            args = shlex.split(user_input)
        except ValueError as e:
            print(f"Ошибка парсинга: {e}")
            continue

        cmd = args[0].lower()

        # выход
        if cmd in {"exit", "quit"}:
            print("До встречи!")
            break

        # помощь
        if cmd == "help":
            print(HELP_TEXT)
            continue

        # show tables
        if cmd == "show" and len(args) >= 2 and args[1].lower() == "tables":
            _print_tables(metadata)
            continue

        # describe <table>
        if cmd == "describe":
            if len(args) < 2:
                print("Укажите имя таблицы: describe <table>")
                continue
            _describe_table(metadata, args[1])
            continue

        # create <table> <col:type>...
        if cmd == "create":
            if len(args) < 3:
                print("Синтаксис: create <table> <col:type> [col:type ...]")
                continue
            table_name = args[1]
            try:
                cols = _parse_columns(args[2:])
                metadata = create_table(metadata, table_name, cols)
                save_metadata(METADATA_FILE, metadata)
                print(f"Таблица '{table_name}' создана.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # drop <table>
        if cmd == "drop":
            if len(args) < 2:
                print("Синтаксис: drop <table>")
                continue
            table_name = args[1]
            try:
                metadata = drop_table(metadata, table_name)
                save_metadata(METADATA_FILE, metadata)
                print(f"Таблица '{table_name}' удалена.")
            except Exception as e:
                print(f"Ошибка: {e}")
            continue

        # неизвестная команда
        print("Неизвестная команда. Введите 'help'.")
