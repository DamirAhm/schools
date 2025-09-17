## Сборщик школ по странам СНГ (Yandex Maps Search API)

Скрипт обходит выбранную страну тайлами, запрашивает Yandex Maps Search API по запросу «школа», агрегирует, фильтрует и дедуплицирует результаты, сохраняет в CSV и JSON.

Поддерживаемые страны (код через `--country`): `KG`, `KZ`, `RU`, `BY`, `UA`, `UZ`, `TJ`, `TM`, `AM`, `AZ`, `GE`.

### Вариант А: для новичков (ничего не установлено)

1) Установите Python (Windows/Mac/Linux):
   - Windows: скачайте установщик Python 3.10+ с сайта `https://www.python.org/downloads/`, при установке поставьте галочку «Add Python to PATH».
   - macOS: установите `python3` через `https://www.python.org/downloads/` или `brew install python`.
   - Linux (Ubuntu/Debian): выполните в терминале:
     ```bash
     sudo apt update && sudo apt install -y python3 python3-pip
     ```

2) Скачайте/получите ваш API-ключ Яндекса (Search API) и запишите его.

3) Откройте терминал/командную строку и выполните команды:
   ```bash
   cd /home/damirahm/home/kirill
   python3 -m pip install --user -r requirements.txt
   echo 'YANDEX_MAPS_API_KEY=ВАШ_КЛЮЧ' > .env
   ```

4) Запустите сбор (пример для Кыргызстана):
   ```bash
   python3 scripts/get_kg_schools.py --country KG --tiles 8 --strict --output-dir output
   ```

5) Результаты появятся в папке `output/` в виде двух файлов:
   - `<country>_schools.csv`
   - `<country>_schools.json`

### Вариант B: через виртуальное окружение (рекомендуется для разработчиков)

```bash
cd /home/damirahm/home/kirill
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
echo 'YANDEX_MAPS_API_KEY=ВАШ_КЛЮЧ' > .env
python scripts/get_kg_schools.py --country KG --tiles 8 --strict --output-dir output
```

### Аргументы CLI

- `--country`: код страны (см. список выше). По умолчанию `KG`.
- `--tiles`: число тайлов по оси (чем больше — тем больше охват и запросов). По умолчанию 8.
- `--output-dir`: папка вывода. По умолчанию `./output`.
- `--strict/--no-strict`: строгая фильтрация общеобразовательных школ (по умолчанию включена).
- `--api-key`: можно передать ключ явно (иначе берётся из переменной `YANDEX_MAPS_API_KEY` или `.env`).
- `--lang`: язык результатов, по умолчанию `ru_RU`.
- `--interactive`: интерактивный режим с меню выбора.

Примеры:

```bash
# Казахстан, больше тайлов
python3 scripts/get_kg_schools.py --country KZ --tiles 12 --strict --output-dir output

# Россия, без строгого фильтра
python3 scripts/get_kg_schools.py --country RU --no-strict --output-dir output

# Передать ключ напрямую без .env
python3 scripts/get_kg_schools.py --country KG --api-key 62669142-15c5-4909-9982-a8e7ef838645

# Интерактивный режим (меню)
# Теперь по умолчанию: просто запустите без аргументов
python3 scripts/get_kg_schools.py
```

### Что делает фильтрация и дедупликация

- Фильтрация: исключает автошколы, спортивные/танцевальные/языковые школы и т.п.; учитывает категории и название.
- Дедупликация: совмещает по `id`, по нормализованным `name+address` и по `name+координаты` (округление до 4 знаков).

### Советы по полноте данных

- Увеличьте `--tiles` (например, до 12–16), это повышает шансы собрать "узкие" результаты.
- Можно повторить запуск с альтернативными запросами (см. код), например, "гимназия", "лицей". Сейчас используется базовый запрос «школа».

