# parser.py
"""
Парсеры значений и условий для простых SQL-подобных команд.
Требование: строковые значения обязательно в кавычках.
Поддерживаемые типы значений: int, bool(true/false), str.
"""

from typing import Any, Dict, List, Tuple
import shlex

TRUE_LIT = "true"
FALSE_LIT = "false"


def parse_value(text: str) -> Any:
    """
    Преобразует строку в Python-значение:
      - true/false (любой регистр) -> bool
      - целое число (знак допускается) -> int
      - иначе — строка (снимать кавычки/экранирование помогает shlex)
    ВАЖНО: строковые значения должны быть в кавычках.
    """
    t = text.strip()

    # снимем внешние кавычки (если есть) через shlex
    lx = shlex.shlex(t, posix=True)
    lx.whitespace_split = True
    toks = [tok for tok in lx]

    if len(toks) == 1:
        tok = toks[0]
        low = tok.lower()

        if low == TRUE_LIT:
            return True
        if low == FALSE_LIT:
            return False

        # int?
        if tok.isdigit() or (tok[0] in "+-" and tok[1:].isdigit()):
            try:
                return int(tok)
            except ValueError:
                pass

        # если токен без кавычек и не число/булево — считаем это ОШИБКОЙ
        if not (t.startswith("'") or t.startswith('"')):
            raise ValueError(f"Строковые значения должны быть в кавычках: {t!r}")  # noqa: E501
        # иначе это строка
        return tok

    # если токенов больше одного
    # требуем кавычки
    if not (t.startswith("'") or t.startswith('"')):
        raise ValueError(f"Строковые значения с пробелами требуются в кавычках: {t!r}")  # noqa: E501

    # соберём строку обратно
    return " ".join(toks)


def parse_assignment(text: str) -> Tuple[str, Any]:
    """
    Разбирает выражение вида:  key = value
    Возвращает (key, parsed_value).
    Пробелы вокруг '=' необязательны.
    """
    if "=" not in text:
        raise ValueError(f"Ожидалось присваивание вида key = value, получено: {text!r}")  # noqa: E501
    left, right = text.split("=", 1)
    key = left.strip()
    if not key:
        raise ValueError(f"Пустое имя столбца в присваивании: {text!r}")
    value = parse_value(right.strip())
    return key, value


def _split_clauses(clause: str) -> List[str]:
    """
    Делит строку условий на части по запятым ИЛИ по 'and'/'AND'/'And'…
    вне кавычек.
    Пример: "age = 28, active = true"  или  "age=28 AND active=true"
    """
    s = clause.strip()
    if not s:
        return []

    # сначала попробуем безопасно порезать по запятым с учётом кавычек
    parts: List[str] = []
    buf: List[str] = []
    in_single = False
    in_double = False

    i = 0
    while i < len(s):
        ch = s[i]
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

    # если запятых не было, попробуем разделить по AND (вне кавычек)
    if len(parts) == 1 and "and" in parts[0].lower():
        tmp = parts[0]
        parts = []
        buf = []
        in_single = False
        in_double = False
        i = 0
        while i < len(tmp):
            # проверим слово 'and' (без регистра) вне кавычек
            if (
                    not in_single and not in_double and
                    i + 3 <= len(tmp) and tmp[i:i+3].lower() == "and"
            ):
                # убедимся, что по краям слово разделено пробелом/краем строки
                prev = tmp[i-1] if i-1 >= 0 else " "
                nxt = tmp[i+3] if i+3 < len(tmp) else " "

                if prev.isspace() and nxt.isspace():
                    # закрываем буфер в часть
                    parts.append("".join(buf).strip())
                    buf = []
                    i += 3
                    continue
            ch = tmp[i]
            if ch == "'" and not in_double:
                in_single = not in_single
                buf.append(ch)
            elif ch == '"' and not in_single:
                in_double = not in_double
                buf.append(ch)
            else:
                buf.append(ch)
            i += 1
        if buf:
            parts.append("".join(buf).strip())

    # отфильтруем пустые элементы
    return [p for p in parts if p]


def parse_where(where_str: str) -> Dict[str, Any]:
    """
    Разбирает where-строку в dict: "age = 28 AND active = true" ->
    {"age": 28, "active": True}
    Допускаются запятые-делители: "age=28, active=true".
    """
    if where_str is None or where_str.strip() == "":
        return {}
    parts = _split_clauses(where_str)
    out: Dict[str, Any] = {}
    for part in parts:
        key, val = parse_assignment(part)
        out[key] = val
    return out


def parse_set(set_str: str) -> Dict[str, Any]:
    """
    Разбирает set-строку в dict: "name='Bob', active=true" ->
    {"name": "Bob", "active": True}
    """
    if not set_str or set_str.strip() == "":
        raise ValueError("SET-часть пуста.")
    parts = _split_clauses(set_str)
    if not parts:
        raise ValueError("Не найдено ни одного присваивания в SET.")
    out: Dict[str, Any] = {}
    for part in parts:
        key, val = parse_assignment(part)
        out[key] = val
    return out
