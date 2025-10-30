from __future__ import annotations

import time
from functools import wraps
from typing import Any, Callable, Optional


def handle_db_errors(
    fallback: Optional[Callable[..., Any]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def _wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except FileNotFoundError:
                print(
                    "Ошибка: Файл данных не найден. "
                    "Возможно, база данных ещё не инициализирована."
                )
            except KeyError as e:
                print(f"Ошибка: Таблица или столбец '{e}' не найден.")
            except ValueError as e:
                print(f"Ошибка валидации: {e}")
            except Exception as e:
                print(f"Произошла непредвиденная ошибка: {e}")
            return fallback(*args, **kwargs) if callable(fallback) else None

        return _wrapper

    return _decorator


def confirm_action(
    action_name: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def _wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                answer = input(
                    (
                        'Вы уверены, что хотите выполнить '
                        f'"{action_name}"? [y/n]: '
                    )
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("Операция отменена.")
                return args[0] if args else None

            if answer not in {"y", "yes", "д", "да"}:
                print("Операция отменена пользователем.")
                return args[0] if args else None

            return func(*args, **kwargs)

        return _wrapper

    return _decorator


def log_time(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Замеряет время выполнения функции и печатает:
    'Функция <имя_функции> выполнилась за X.XXX секунд.'
    """
    @wraps(func)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.monotonic()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.monotonic() - start
            print(
                f"Функция {func.__name__} выполнилась "
                f"за {elapsed:.3f} секунд."
            )

    return _wrapper
