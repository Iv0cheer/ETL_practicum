# Лабораторная работа №5.1
## 
### Вариант 19.

| Вариант | Задание 1 (Анализ/ETL) | Задание 2 (Обработка/Логика) | Задание 3 (Отчетность/Метрики) |
|----------|---------|---------|---------|
| 19 | Анализ расширения DAG (новые типы данных) |	Оптимизация хранения (сжатие/структура папок) |	Возможность выполнения на удаленных воркерах |


1. Права доступа к папкам

<img width="968" height="73" alt="image" src="https://github.com/user-attachments/assets/b4b1fee4-d72d-4eb7-983b-ef6a53b3bc51" />

2. Сборка и запуск Docker Compose

<img width="1388" height="274" alt="image" src="https://github.com/user-attachments/assets/443e081c-3f4b-4e9f-b187-4e44fff3d8ac" />

3. Запуск контейнеров `docker compose up -d`

<img width="884" height="223" alt="image" src="https://github.com/user-attachments/assets/42d02fe7-ee0d-452d-83e0-d5d66d3b273b" />

4. Написание DAG (модификация DAG'а)

<details>
  <summary>КЛИК СЮДА ДЛЯ КОДА</summary>

```python
import json
import pathlib
import gzip
import shutil
from datetime import datetime, timedelta
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
# Папка для сжатых архивов (Задание 2: оптимизация хранения)
ARCHIVE_DIR = f"{DATA_DIR}/archives"
# Временный файл для JSON внутри контейнера
TMP_JSON_FILE = "/tmp/launches.json"
# Ограничиваем количество картинок на одну прогонку DAG (ускоряет и дает прогресс).
MAX_IMAGES = 3
# URL API (Swagger для v2.3.0: список upcoming запусков)
# Важно: в v2.3.0 endpoint называется `/launches/upcoming/`, а не `/launch/upcoming/`.
# Для ускорения и уменьшения размера ответа используем `mode=list`
# (в таком режиме JSON значительно компактнее, но `image.image_url` сохраняется).
API_URL = f"https://ll.thespacedevs.com/2.3.0/launches/upcoming/?format=json&mode=list&limit={MAX_IMAGES}"

# --- Задание 1: Анализ расширения DAG (новые типы данных) ---
# Добавляем дополнительные метаданные о запусках для анализа
# Извлекаем информацию о миссиях, датах и статусах

def extract_mission_metadata():
    """Извлечение дополнительных метаданных из API для анализа новых типов данных"""
    metadata_file = f"{DATA_DIR}/mission_metadata.json"
    metadata_list = []
    
    if not pathlib.Path(TMP_JSON_FILE).exists():
        print(f"JSON файл {TMP_JSON_FILE} не найден")
        return
    
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        try:
            launches = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON: {e}")
            return
    
    for launch in launches.get("results", []):
        metadata = {
            "mission_name": launch.get("name", "Unknown"),
            "launch_date": launch.get("net", launch.get("window_start")),
            "status": launch.get("status", {}).get("name") if isinstance(launch.get("status"), dict) else launch.get("status"),
            "rocket_config": launch.get("rocket", {}).get("configuration", {}).get("name") if isinstance(launch.get("rocket"), dict) else None,
            "launch_service_provider": launch.get("launch_service_provider", {}).get("name") if isinstance(launch.get("launch_service_provider"), dict) else None,
            "pad_location": launch.get("pad", {}).get("location", {}).get("name") if isinstance(launch.get("pad"), dict) else None,
        }
        metadata_list.append(metadata)
    
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata_list, f, indent=2, ensure_ascii=False)
    
    print(f"Сохранены метаданные о {len(metadata_list)} миссиях в {metadata_file}")

# --- Задание 2: Оптимизация хранения (сжатие/структура папок) ---
# Функция для сжатия старых изображений в архив

def compress_old_images():
    """Сжатие изображений старше 7 дней для оптимизации хранения"""
    import time
    from datetime import datetime, timedelta
    
    # Создаем папку для архивов если её нет
    pathlib.Path(ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)
    
    # Получаем все изображения
    image_extensions = ["*.jpg", "*.jpeg", "*.png", "*.gif"]
    all_images = []
    for ext in image_extensions:
        all_images.extend(pathlib.Path(IMAGES_DIR).glob(ext))
    
    if not all_images:
        print("Нет изображений для сжатия")
        return
    
    # Определяем дату 7 дней назад
    week_ago = time.time() - (7 * 24 * 3600)
    
    # Создаем архивный файл с датой
    archive_name = f"images_archive_{datetime.now().strftime('%Y%m%d')}.tar.gz"
    archive_path = pathlib.Path(ARCHIVE_DIR) / archive_name
    
    # Собираем старые файлы
    old_images = [img for img in all_images if img.stat().st_mtime < week_ago]
    
    if not old_images:
        print("Нет изображений старше 7 дней для архивации")
        return
    
    # Создаем gzip архив
    import tarfile
    with tarfile.open(archive_path, "w:gz") as tar:
        for img in old_images:
            tar.add(img, arcname=img.name)
            # Удаляем оригинал после добавления в архив
            img.unlink()
    
    # Сохраняем информацию об архиве
    archive_info = {
        "archive_name": archive_name,
        "creation_date": datetime.now().isoformat(),
        "files_count": len(old_images),
        "files_list": [img.name for img in old_images]
    }
    
    info_file = ARCHIVE_DIR / f"{archive_name}.info.json"
    with open(info_file, "w", encoding="utf-8") as f:
        json.dump(archive_info, f, indent=2)
    
    print(f"Создан архив {archive_name} с {len(old_images)} изображениями")

# --- Определение DAG ---
# Задание 3: Добавляем параметры для удаленных воркеров
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # Задание 3: Параметры для выполнения на удаленных воркерах (используем timedelta)
    'execution_timeout': timedelta(hours=1),  # Таймаут выполнения задачи - 1 час
    'retry_delay': timedelta(minutes=5),  # Задержка между повторами - 5 минут
}

dag = DAG(
    dag_id="variant_19_dag.py",
    description="Cleans the entire data directory, then downloads rocket pictures.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    # Задание 3: Параметры для распределенного выполнения
    max_active_runs=1,  # Ограничиваем количество одновременных запусков
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
                response = requests.get(image_url, timeout=30)
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
    dag=dag
)

# Задание 1: Задача для извлечения метаданных миссий
extract_metadata = PythonOperator(
    task_id="extract_mission_metadata",
    python_callable=extract_mission_metadata,
    dag=dag
)

# Задание 2: Задача для сжатия старых изображений
compress_images = PythonOperator(
    task_id="compress_old_images",
    python_callable=compress_old_images,
    dag=dag
)

# 4. ЗАДАЧА УВЕДОМЛЕНИЯ. Сообщаем о результате
notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "There are now $(ls {IMAGES_DIR}/ | wc -l) images in {IMAGES_DIR}." && echo "Archive info: $(ls {ARCHIVE_DIR}/ 2>/dev/null | wc -l) archive files"',
    dag=dag,
)

# --- Порядок выполнения ---
# Сначала чистим, потом скачиваем JSON, потом скачиваем картинки, потом метаданные, потом сжатие, потом уведомляем
clean_data_directory >> download_launches >> get_pictures >> extract_metadata >> compress_images >> notify
```
  
</details>

5. Запуск DAG'а

<img width="2308" height="585" alt="image" src="https://github.com/user-attachments/assets/52372074-6afe-4345-9466-b1dd7760440e" />

Как видим DAG проработал полностью правильно, кроме проблемы с загрузкой изображений. Не работает даже в DAG'е выданном ранее

<img width="2224" height="425" alt="image" src="https://github.com/user-attachments/assets/beeb2b88-d378-4390-969c-10944eb77168" />

6. Стримлит

По итогу я загрузил заглушки в папку images, иначе не получилось. JSON - не отработал тоже...

<img width="2310" height="1217" alt="image" src="https://github.com/user-attachments/assets/dd6db1e3-714f-405c-af69-5c52cdb20ca2" />

[Модифицированный файл ml.ipynb](/ml_copy.ipynb)

7. Что было изменено:

ЗАДАНИЕ ПЕРВОЕ. Анализ расширения DAG (новые типы данных)

<details><summary>Изменения в коде (модификации)</summary>

* Основной файл DAG (variant_19_dag.py)

```python
def extract_mission_metadata():
    """Извлечение дополнительных метаданных из API для анализа новых типов данных"""
    metadata_file = f"{DATA_DIR}/mission_metadata.json"
    metadata_list = []
    
    if not pathlib.Path(TMP_JSON_FILE).exists():
        print(f"JSON файл {TMP_JSON_FILE} не найден")
        return
    
    with open(TMP_JSON_FILE, encoding="utf-8") as f:
        try:
            launches = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Ошибка декодирования JSON: {e}")
            return
    
    for launch in launches.get("results", []):
        metadata = {
            "mission_name": launch.get("name", "Unknown"),
            "launch_date": launch.get("net", launch.get("window_start")),
            "status": launch.get("status", {}).get("name") if isinstance(launch.get("status"), dict) else launch.get("status"),
            "rocket_config": launch.get("rocket", {}).get("configuration", {}).get("name") if isinstance(launch.get("rocket"), dict) else None,
            "launch_service_provider": launch.get("launch_service_provider", {}).get("name") if isinstance(launch.get("launch_service_provider"), dict) else None,
            "pad_location": launch.get("pad", {}).get("location", {}).get("name") if isinstance(launch.get("pad"), dict) else None,
        }
        metadata_list.append(metadata)
    
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata_list, f, indent=2, ensure_ascii=False)
    
    print(f"Сохранены метаданные о {len(metadata_list)} миссиях в {metadata_file}")
```

* Задача в DAG

```python
extract_metadata = PythonOperator(
    task_id="extract_mission_metadata",
    python_callable=extract_mission_metadata,
    dag=dag
)

clean_data_directory >> download_launches >> get_pictures >> extract_metadata >> compress_images >> notify
```

</details>

Что делает:

> Извлекает из JSON ответа API дополнительные поля (название миссии, дату запуска, статус, конфигурацию ракеты, провайдера, локацию) и сохраняет в отдельный файл mission_metadata.json.

<img width="954" height="630" alt="image" src="https://github.com/user-attachments/assets/bb545c11-eefd-4bb1-ad90-c21c17bf9a43" />

ЗАДАНИЕ ВТОРОЕ. Оптимизация хранения (сжатие/структура папок)

<details><summary>Изменения в коде (модификации)</summary>

```python
ARCHIVE_DIR = f"{DATA_DIR}/archives"

# ...
```
```python
# ...

def compress_old_images():
    """Сжатие изображений старше 7 дней для оптимизации хранения"""
    import time
    
    # Создаем папку для архивов если её нет
    pathlib.Path(ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)
    
    # Получаем все изображения
    image_extensions = ["*.jpg", "*.jpeg", "*.png", "*.gif"]
    all_images = []
    for ext in image_extensions:
        all_images.extend(pathlib.Path(IMAGES_DIR).glob(ext))
    
    if not all_images:
        print("Нет изображений для сжатия")
        return
    
    # Определяем дату 7 дней назад
    week_ago = time.time() - (7 * 24 * 3600)
    
    # Создаем архивный файл с датой
    archive_name = f"images_archive_{datetime.now().strftime('%Y%m%d')}.tar.gz"
    archive_path = pathlib.Path(ARCHIVE_DIR) / archive_name
    
    # Собираем старые файлы
    old_images = [img for img in all_images if img.stat().st_mtime < week_ago]
    
    if not old_images:
        print("Нет изображений старше 7 дней для архивации")
        return
    
    # Создаем gzip архив
    import tarfile
    with tarfile.open(archive_path, "w:gz") as tar:
        for img in old_images:
            tar.add(img, arcname=img.name)
            # Удаляем оригинал после добавления в архив
            img.unlink()
    
    # Сохраняем информацию об архиве
    archive_info = {
        "archive_name": archive_name,
        "creation_date": datetime.now().isoformat(),
        "files_count": len(old_images),
        "files_list": [img.name for img in old_images]
    }
    
    info_file = ARCHIVE_DIR / f"{archive_name}.info.json"
    with open(info_file, "w", encoding="utf-8") as f:
        json.dump(archive_info, f, indent=2)
    
    print(f"Создан архив {archive_name} с {len(old_images)} изображениями")
```

* Новая задача DAG

```python
compress_images = PythonOperator(
    task_id="compress_old_images",
    python_callable=compress_old_images,
    dag=dag
)
```

</details>

Что делает:

> Создает папку archives/, находит изображения старше 7 дней, сжимает их в tar.gz архив, удаляет оригиналы, сохраняет JSON файл с информацией об архиве

ЗАДАНИЕ ТРЕТЬЕ. Возможность выполнения на удаленных воркерах

<details><summary>Изменения в коде (модификации)</summary>
  
```
def get_worker_metrics():
    """Сбор метрик о воркере, на котором выполняется ML анализ"""
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "worker_hostname": socket.gethostname(),
        "worker_ip": socket.gethostbyname(socket.gethostname()),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": psutil.cpu_count(),
        "cpu_usage_percent": psutil.cpu_percent(interval=1),
        "memory_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "memory_available_gb": round(psutil.virtual_memory().available / (1024**3), 2),
        "memory_usage_percent": psutil.virtual_memory().percent,
        "disk_usage_percent": psutil.disk_usage('/').percent,
        "gpu_available": torch.cuda.is_available(),
        "gpu_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
    }
    
    if torch.cuda.is_available():
        metrics["gpu_name"] = torch.cuda.get_device_name(0)
        metrics["gpu_memory_total_gb"] = round(torch.cuda.get_device_properties(0).total_memory / (1024**3), 2)
    
    return metrics

def log_processing_time(start_time, end_time, num_images):
    """Логирование времени обработки для метрик производительности"""
    duration = end_time - start_time
    metrics = {
        "processing_duration_seconds": duration,
        "images_processed": num_images,
        "average_time_per_image": duration / num_images if num_images > 0 else 0,
        "processing_rate_images_per_second": num_images / duration if duration > 0 else 0
    }
    return metrics

# Сохраняем метрики воркера
worker_metrics = get_worker_metrics()

# ...
# ...
# ...

# Сохранение метрик воркера и производительности в отдельный файл
worker_metrics.update(processing_metrics)
worker_metrics["model_used"] = model_id
worker_metrics["candidate_labels_count"] = len(candidate_labels)

with open("./data/worker_metrics.json", "w") as f:
    json.dump(worker_metrics, f, indent=2)
```

В файле стримлита `app.py`:

```python
# Секция 3: Метрики удаленного воркера (Задание 3)
st.header("3. Метрики выполнения на удаленных воркерах")

if os.path.exists(WORKER_METRICS_FILE):
    with open(WORKER_METRICS_FILE, "r") as f:
        worker_metrics = json.load(f)
    
    # Информация о воркере
    st.subheader("Информация о воркере")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Хост", worker_metrics.get("worker_hostname", "N/A"))
    # ... и так далее для всех метрик
```

</details>

## Трудности и как с ними справлялся

| Трудность | Как решил |
|-----------|-----------|
| Не хватало места на диске на образе | Увеличил через VirualBox до 90 Гб |
| Не качаются фотографии через API из-за того, что ресурс заблокирован | Пытался подключить VPN на виртуалку, не получилось |
| При пересборке билда постоянно сбиваются права на папки | `sudo chmod -R 777 data` |
| Лаги компьютера | Никак, база |


