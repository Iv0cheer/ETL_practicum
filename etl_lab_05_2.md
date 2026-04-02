# Лабораторная работа №5.1
## 
### Вариант 19.

| Параметр | Значение |
|----------|---------|



1. Права доступа к папкам

<img width="968" height="73" alt="image" src="https://github.com/user-attachments/assets/b4b1fee4-d72d-4eb7-983b-ef6a53b3bc51" />

2. Сборка и запуск Docker Compose

<img width="1388" height="274" alt="image" src="https://github.com/user-attachments/assets/443e081c-3f4b-4e9f-b187-4e44fff3d8ac" />

3. Запуск контейнеров `docker compose up -d`

<img width="884" height="223" alt="image" src="https://github.com/user-attachments/assets/42d02fe7-ee0d-452d-83e0-d5d66d3b273b" />

4. Написание DAG (модификация DAG'а)

<details>
  <summary></summary>

```python
import json
import pathlib
import gzip
import shutil
from datetime import datetime
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --- Конфигурационные переменные ---

# Основная папка для данных, которая проброшена на хост в ./data
DATA_DIR = "/opt/airflow/data"
# Вложенная папка для изображений (будет создаваться заново после очистки)
IMAGES_DIR = f"{DATA_DIR}/images"
# Временный файл для JSON внутри контейнера
TMP_JSON_FILE = "/tmp/launches.json"
# Ограничиваем количество картинок на одну прогонку DAG (ускоряет и дает прогресс).
MAX_IMAGES = 2
# URL API (Swagger для v2.3.0: список upcoming запусков)
# Важно: в v2.3.0 endpoint называется `/launches/upcoming/`, а не `/launch/upcoming/`.
# Для ускорения и уменьшения размера ответа используем `mode=list`
# (в таком режиме JSON значительно компактнее, но `image.image_url` сохраняется).
API_URL = f"https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit={MAX_IMAGES}"

# --- Определение DAG ---
dag = DAG(
    dag_id="variant_19_dag",
    description="Cleans the entire data directory, then downloads rocket pictures.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False
)

# --- Определение Задач ---

# 1. ЗАДАЧА ОЧИСТКИ. Удаляем всё содержимое папки /opt/airflow/data
clean_data_directory = BashOperator(
    task_id="clean_data_directory",
    # Надежная команда: сначала убеждаемся, что папка существует, потом удаляем всё ВНУТРИ неё
    bash_command=f"mkdir -p {DATA_DIR} && rm -rf {DATA_DIR}/*",
    dag=dag,
)

# 2. ЗАДАЧА СКАЧИВАНИЯ JSON: Скачиваем свежий список запусков
download_launches = BashOperator(
    task_id="download_launches",
    # `-f` чтобы не скачивать HTML/404 в JSON-файл и не падать потом на json.load.
    bash_command=(
        f"curl -fSL --connect-timeout 15 --max-time 120 --progress-bar "
        f"-H 'Accept: application/json' -o {TMP_JSON_FILE} '{API_URL}'"
    ),
    dag=dag,
)

# 3. ЗАДАЧА СКАЧИВАНИЯ КАРТИНОК. Обрабатываем JSON и загружаем фото
def _get_pictures():
    # Эта команда создаст заново папку /images внутри /data
    pathlib.Path(IMAGES_DIR).mkdir(parents=True, exist_ok=True)
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        try:
            launches = json.load(f)
        except json.JSONDecodeError as e:
            # Чтобы в логах было видно, что прилетело вместо JSON (например, HTML error page).
            f.seek(0)
            preview = f.read(500)
            raise RuntimeError(
                f"Launch API returned non-JSON payload. json error: {e}. Payload preview: {preview!r}"
            ) from e

        # В API v2.3.0 поле `image` - это объект, внутри него лежит `image_url`.
        image_urls = []
        for launch in launches.get("results", []):
            image = launch.get("image")
            if isinstance(image, dict):
                image_url = image.get("image_url")
                if image_url:
                    image_urls.append(image_url)
            elif isinstance(image, str) and image:
                # На случай неожиданных форматов данных.
                image_urls.append(image)

        # Убираем дубликаты, чтобы не скачивать одно и то же.
        image_urls = list(dict.fromkeys(image_urls))[:MAX_IMAGES]
        for image_index, image_url in enumerate(image_urls, start=1):
            print(f"[{image_index}/{len(image_urls)}] Downloading: {image_url}")
            try:
                response = requests.get(image_url, timeout=60)
                response.raise_for_status()
                image_filename = image_url.split("/")[-1]
                target_file = f"{IMAGES_DIR}/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.RequestException as e:
                print(f"Could not download {image_url}: {e}")

get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
    retries=2,
)

# 4. ЗАДАЧА УВЕДОМЛЕНИЯ. Сообщаем о результате
notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {IMAGES_DIR}/ | wc -l) images in {IMAGES_DIR}."',
    dag=dag,
)

# --- Задание 2: Оптимизация хранения (сжатие/структура папок) ---
def _compress_and_organize():
    current_date = datetime.now()
    year_month = current_date.strftime("%Y-%m")
    archive_dir = f"{DATA_DIR}/archive/{year_month}"
    pathlib.Path(archive_dir).mkdir(parents=True, exist_ok=True)
    
    compressed_file = f"{DATA_DIR}/launches.json.gz"
    with open(TMP_JSON_FILE, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    archive_file = f"{archive_dir}/launches_{current_date.strftime('%Y%m%d_%H%M%S')}.json.gz"
    shutil.copy2(compressed_file, archive_file)
    pathlib.Path(TMP_JSON_FILE).unlink()

compress_and_organize = PythonOperator(
    task_id="compress_and_organize",
    python_callable=_compress_and_organize,
    dag=dag,
)

# --- Задание 3: Возможность выполнения на удаленных воркерах ---
def _upload_to_remote():
    from airflow.models import Variable
    remote_enabled = Variable.get("REMOTE_STORAGE_ENABLED", default_var="false").lower() == "true"
    if remote_enabled:
        print("[Задание 3] Данные готовы для выгрузки на удаленное хранилище")

remote_upload = PythonOperator(
    task_id="remote_upload",
    python_callable=_upload_to_remote,
    dag=dag,
)

# --- Порядок выполнения ---
# Сначала чистим, потом скачиваем JSON, потом скачиваем картинки, потом уведомляем
clean_data_directory >> download_launches >> get_pictures >> compress_and_organize >> notify >> remote_upload
```
  
</details>

5. Запуск DAG'а

<img width="2308" height="585" alt="image" src="https://github.com/user-attachments/assets/52372074-6afe-4345-9466-b1dd7760440e" />

Как видим DAG проработал полностью правильно, кроме проблемы с загрузкой изображений. Не работает даже в DAG'е выданном ранее

<img width="2224" height="425" alt="image" src="https://github.com/user-attachments/assets/beeb2b88-d378-4390-969c-10944eb77168" />








| Трудность | Как решил |
|-----------|-----------|
| Не хватало места на диске на образе | Увеличил через VirualBox до 90 Гб |
| Не качаются фотографии через API из-за того, что ресурс заблокирован | Пытался подключить VPN на виртуалку, не получилось |
