# Модуль «База данных»

Простая **файловая база данных** на Python.  
Поддерживает создание/просмотр/удаление таблиц и SQL-подобные операции над данными: `insert`, `select`, `update`, `delete`.  
Метаданные хранятся в `storage/metadata.json`, строки таблиц — в `data/<table>.json`.

---

## 🚀 Установка и запуск

```bash
poetry install
poetry run project

# или:
poetry shell
project
```

После старта появится интерактивная консоль:
```
Первая попытка запустить проект!

***
<command> exit - выйти из программы
<command> help - справочная информация
```

Переменные окружения (необязательно):
- `PRIMITIVE_DB_META` — путь к метаданным (по умолчанию `storage/metadata.json`)
- `PRIMITIVE_DB_DATA_DIR` — каталог данных (по умолчанию `data/`)

---

## ⚙️ Возможности

- Создание таблиц с типами `int`, `str`, `bool` (столбец `ID:int` добавляется автоматически)
- Просмотр списка/структуры таблиц
- Удаление таблиц (с подтверждением)
- CRUD по строкам: `insert`, `select`, `update`, `delete`
- Форматированный вывод `select` через **PrettyTable**
- Автосохранение в JSON

### Новое и полезное
- ✅ **Единая обработка ошибок**: декоратор `handle_db_errors`
- 🛡️ **Подтверждение опасных действий**: декоратор `confirm_action` (для `drop_table` и `delete`)
- ⏱️ **Замер времени**: декоратор `log_time` (для «медленных» операций `insert` и `select`)
- ⚡ **Кэширование результатов** `select` (инвалидация при `insert`, `update`, `delete`)

---

## 📂 Структура проекта

```
project2_demyanova/
├── src/
│   └── primitive_db/
│       ├── __init__.py
│       ├── core.py          # логика таблиц и CRUD (+декораторы, кэш)
│       ├── decorators.py    # handle_db_errors, confirm_action, log_time
│       ├── parser.py        # разбор WHERE/SET/значений
│       ├── utils.py         # I/O: metadata + data/<table>.json
│       ├── engine.py        # цикл команд, печать таблиц
│       └── main.py          # точка входа (скрипт `project`)
├── storage/                 # создаётся автоматически
├── data/                    # создаётся автоматически
├── tests/                   # pytest
├── pyproject.toml
├── Makefile
└── README.md
```

---

## 🔖 Команды

### Создание таблицы
```
create <table> <col:type> [col:type ...]
```
Пример:
```
create users name:str age:int is_active:bool
```
Столбец `ID` добавляется автоматически.

### Список таблиц
```
show tables
```

### Структура таблицы
```
describe <table>
```

### Удаление таблицы (с подтверждением)
```
drop <table>
```
CLI спросит:
```
Вы уверены, что хотите выполнить "удаление таблицы"? [y/n]:
```

---

## 🧾 CRUD-операции с данными

> Строковые значения — в кавычках, булевы — `true/false`.

### INSERT
```
insert into <table> values (<v1>, <v2>, ...)
```
Пример:
```
insert into users values ("Sergei", 28, true)
```
Вывод:
```
OK. Добавлено. Всего записей: 1
Функция insert выполнилась за 0.002 секунд.
```

### SELECT
```
select from <table>
select from <table> where <col> = <value>
```
Пример:
```
select from users where age = 28
```
Вывод:
```
+----+--------+-----+-----------+
| ID |  name  | age | is_active |
+----+--------+-----+-----------+
| 1  | Sergei |  28 |    True   |
+----+--------+-----+-----------+
Функция select выполнилась за 0.001 секунд.
```

### UPDATE
```
update <table> set <col1> = <value1> [ , ... ] [where <col> = <value>]
```
Пример:
```
update users set is_active = false where name = "Sergei"
```
Вывод:
```
OK. Обновлено: 1 строк(и).
```

### DELETE (с подтверждением)
```
delete from <table> where <col> = <value>
```
Пример:
```
delete from users where name = "Sergei"
```
Подтверждение:
```
Вы уверены, что хотите выполнить "удаление строк"? [y/n]:
```
Вывод:
```
OK. Удалено: 1 строк(и).
```

### INFO
```
info <table>
```
Печатает служебную информацию: колонки, число строк, следующий ID.

---

## 🧰 Сообщения об ошибках (примеры)

```
Ошибка валидации: Неверный тип для 'age': ожидался int, получен str.
Ошибка: Таблица 'users' не существует.
Ошибка: Файл данных не найден. Возможно, база данных ещё не инициализирована.
Произошла непредвиденная ошибка: <сообщение>
```

---

## 🎥 Демонстрация (asciinema)

### Вариант A — интерактивно
```bash
pipx install asciinema   # или: sudo apt-get install asciinema
asciinema rec -t "Primitive DB: decorators demo" demo.cast
project
# Выполните:
# create users name:str age:int is_active:bool
# insert into users values ("Sergei", 28, true)
# select from users
# delete from users where name = "Sergei"   # ответьте: n, затем повторите и ответьте: y
# drop users                                # ответьте: y
# exit
# затем завершите запись (Ctrl+D / exit)
asciinema upload demo.cast
```

### Вариант B — сценарий без ручного ввода
```bash
cat > demo.sh <<'SH'
#!/usr/bin/env bash
set -e
project <<'CMDS'
create users name:str age:int is_active:bool
insert into users values ("Sergei", 28, true)
select from users
delete from users where name = "Sergei"
n
delete from users where name = "Sergei"
y
drop users
y
exit
CMDS
SH
chmod +x demo.sh
asciinema rec -t "Primitive DB: decorators demo" demo.cast -c "./demo.sh"
```

---

## 🧪 Тесты и линт

```bash
# тесты
poetry run pytest -q

# линт и стиль
poetry run ruff check .
poetry run flake8 .
poetry run black .
```

Рекомендуемые настройки для flake8:
```toml
[tool.flake8]
max-line-length = 79
extend-ignore = ["E203", "W503"]
```

---

## ❓ FAQ

**Почему `select` иногда быстрый «второй раз»?**  
Работает кэширование; при изменениях (`insert/update/delete`) кэш инвалидируется.

**Можно ли перенести CLI на другую машину как `.exe`?**  
Проект поставляется как Python-пакет со скриптом `project` (Poetry entry point).  
Для «одного файла» используйте сборку через PyInstaller/Nuitka на своей ОС.
