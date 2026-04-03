import streamlit as st
import pandas as pd
import json
import os
from PIL import Image
from datetime import datetime

st.set_page_config(page_title="Rocket Launch Analytics", layout="wide")

DATA_DIR = "/opt/airflow/data"
JSON_FILE = f"{DATA_DIR}/launches.json"
PREDICTIONS_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"
WORKER_METRICS_FILE = f"{DATA_DIR}/worker_metrics.json"

st.title("Аналитика космических запусков и ML распознавание")

# Секция 1: Анализ расписания (из JSON)
st.header("1. Ближайшие запуски (ETL Data)")
if os.path.exists(JSON_FILE):
    with open(JSON_FILE, "r") as f:
        launches = json.load(f).get("results", [])
    
    if launches:
        df_launches = pd.DataFrame([{
            "Имя миссии": l.get("name"),
            "Статус": l.get("status", {}).get("name"),
            "Окно старта": l.get("window_start"),
            "Провайдер": l.get("launch_service_provider", {}).get("name")
        } for l in launches])
        st.dataframe(df_launches)
        
        st.subheader("Запуски по провайдерам")
        provider_counts = df_launches["Провайдер"].value_counts()
        st.bar_chart(provider_counts)
else:
    st.warning("Файл launches.json еще не загружен. Запустите DAG в Airflow.")

st.markdown("---")

# Секция 2: Результаты ML
st.header("2. Распознавание типов ракет (ML Data)")
if os.path.exists(PREDICTIONS_FILE):
    if os.path.getsize(PREDICTIONS_FILE) > 0:
        try:
            df_preds = pd.read_csv(PREDICTIONS_FILE)
            if not df_preds.empty:
                st.dataframe(df_preds)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("Статистика по типам ракет")
                    rocket_counts = df_preds["predicted_rocket"].value_counts()
                    st.bar_chart(rocket_counts)
                    
                # Галерея
                st.subheader("Галерея распознанных ракет")
                cols = st.columns(3)
                for idx, row in df_preds.iterrows():
                    img_path = os.path.join(IMAGES_DIR, row['image_name'])
                    if os.path.exists(img_path):
                        with cols[idx % 3]:
                            img = Image.open(img_path)
                            st.image(img, caption=f"{row['predicted_rocket']} ({row['confidence']}%)", use_column_width=True)
            else:
                st.warning("Файл ml_predictions.csv пуст. Запустите ML анализ в Jupyter Notebook.")
        except pd.errors.EmptyDataError:
            st.warning("Файл ml_predictions.csv пуст. Запустите ML анализ в Jupyter Notebook.")
    else:
        st.warning("Файл ml_predictions.csv пуст. Запустите ML анализ в Jupyter Notebook.")
else:
    st.info("Результаты ML еще не готовы. Выполните ноутбук ml.ipynb для генерации прогнозов.")

st.markdown("---")

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
    with col2:
        st.metric("IP адрес", worker_metrics.get("worker_ip", "N/A"))
    with col3:
        st.metric("Платформа", worker_metrics.get("platform", "N/A")[:30])
    with col4:
        st.metric("Python версия", worker_metrics.get("python_version", "N/A"))
    
    # Производительность
    st.subheader("Производительность ML анализа")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        proc_duration = worker_metrics.get("processing_duration_seconds", 0)
        st.metric("Время обработки", f"{proc_duration:.2f} сек")
    with col2:
        st.metric("Обработано изображений", worker_metrics.get("images_processed", 0))
    with col3:
        st.metric("Скорость обработки", f"{worker_metrics.get('processing_rate_images_per_second', 0):.2f} изоб/сек")
    with col4:
        st.metric("Среднее время на изображение", f"{worker_metrics.get('average_time_per_image', 0):.3f} сек")
    
    # Системные ресурсы
    st.subheader("Системные ресурсы воркера")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("CPU использование", f"{worker_metrics.get('cpu_usage_percent', 0)}%")
    with col2:
        st.metric("Память", f"{worker_metrics.get('memory_usage_percent', 0)}%")
    with col3:
        st.metric("Доступно RAM", f"{worker_metrics.get('memory_available_gb', 0)} / {worker_metrics.get('memory_total_gb', 0)} GB")
    with col4:
        st.metric("Disk использование", f"{worker_metrics.get('disk_usage_percent', 0)}%")
    
    # GPU информация (если есть)
    if worker_metrics.get("gpu_available"):
        st.subheader("GPU информация")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("GPU модель", worker_metrics.get("gpu_name", "N/A")[:30])
        with col2:
            st.metric("GPU память", f"{worker_metrics.get('gpu_memory_total_gb', 0)} GB")
        with col3:
            st.metric("GPU количество", worker_metrics.get("gpu_count", 0))
    
    # ML модель
    st.subheader("Параметры ML модели")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Модель", worker_metrics.get("model_used", "N/A"))
    with col2:
        st.metric("Классов для распознавания", worker_metrics.get("candidate_labels_count", 0))
    
    # Время выполнения
    timestamp = worker_metrics.get("timestamp", "")
    if timestamp:
        try:
            dt = datetime.fromisoformat(timestamp)
            st.caption(f"Метрики собраны: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
        except:
            st.caption(f"Метрики собраны: {timestamp}")
    
else:
    st.info("Метрики воркера еще не доступны. Запустите ML анализ в Jupyter Notebook (ml.ipynb) для генерации метрик.")
