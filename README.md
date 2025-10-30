Модуль “База данных”

Этот проект реализует **простую файловую базу данных** на Python.  
Он позволяет создавать, просматривать и удалять таблицы, а также (в будущем) добавлять и редактировать записи.  
Все метаданные и данные хранятся в виде JSON-файлов.

---

## 🚀 Установка и запуск

```bash
poetry install
poetry run project

или 

poetry shell
project
```

После запуска появится интерактивная консоль:
```
Первая попытка запустить проект!

***
<command> exit - выйти из программы
<command> help - справочная информация
```

---

## ⚙️ Основные возможности

- Создание таблиц с типами данных (`int`, `str`, `bool`)
- Просмотр и удаление таблиц
- Просмотр структуры таблицы
- Автоматическое сохранение в `storage/metadata.json`
- Подготовка к CRUD-операциям (insert, select, update, delete)

---

## 📂 Структура проекта

```
project2_demyanova/
│
├── src/
│   └── primitive_db/
│       ├── __init__.py
│       ├── core.py          # Логика работы с таблицами
│       ├── io_utils.py      # Загрузка и сохранение данных
│       ├── engine.py        # Цикл команд, ввод пользователя
│       └── main.py          # Точка входа
│
├── storage/
│   └── metadata.json        # Файл для хранения таблиц и их структур
│
├── pyproject.toml
├── Makefile
└── README.md
```

---

## 🔖 Управление таблицами

### 🛠️ Создание таблицы
```bash
create <table_name> <column_name:type> [column_name:type ...]
```
**Пример:**
```bash
create users name:str age:int is_active:bool
```
Столбец `ID:int` добавляется автоматически.

---

### 📋 Просмотр списка таблиц
```bash
show tables
```
Пример:
```
Список таблиц:
  - users
  - products
```

---

### 🔍 Просмотр структуры таблицы
```bash
describe <table_name>
```
Пример:
```
describe users
```
Результат:
```
Таблица: users
Столбцы:
  ID: int
  name: str
  age: int
  is_active: bool
```

---

### 🗑️ Удаление таблицы
```bash
drop <table_name>
```
Пример:
```
drop users
```
Результат:
```
Таблица 'users' удалена.
```

---

## 💾 Работа с записями *(планируемый функционал)*

### ➕ Добавление записи
```bash
insert <table_name> <column1=value1> <column2=value2> ...
```
**Пример:**
```bash
insert users name="Татьяна" age=25 is_active=true
```
Результат:
```
Добавлена запись с ID=1 в таблицу 'users'.
```

---

### 🔎 Просмотр данных
```bash
select <table_name> [where <column=value>]
```
**Пример:**
```bash
select users
select users where is_active=true
```
Результат:
```
ID | name     | age | is_active
---+----------+-----+-----------
1  | Татьяна  | 25  | True
```

---

### ✏️ Обновление записи
```bash
update <table_name> set <column=value> where <column=value>
```
**Пример:**
```bash
update users set is_active=false where name="Татьяна"
```
Результат:
```
Обновлено 1 значение в таблице 'users'.
```

---

### ❌ Удаление записи
```bash
delete from <table_name> where <column=value>
```
**Пример:**
```bash
delete from users where ID=1
```
Результат:
```
Удалена 1 за

### Операции с данными

Функции:
<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись.
<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию.
<command> select from <имя_таблицы> - прочитать все записи.
<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия> - обновить запись.
<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить запись.
<command> info <имя_таблицы> - вывести информацию о таблице.
<command> exit - выход из программы
<command> help- справочная информация

>>>Введите команду: _