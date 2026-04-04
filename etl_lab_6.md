# Лабораторная работа №5.1
## 
### Вариант 19.

| Вариант | Подзадача 1. Сущности (TARGET_PAGES) | Подзадача 2. Аналитический SQL-запрос (для отчета) | Подзадача 3: График в Streamlit |
|----------|---------|---------|---------|
| 19 | Мессенджеры: WhatsApp, Telegram, WeChat. |	Вывести средние просмотры Telegram. |	Столбчатая диаграмма (Bar Chart). |



## Назначение прав

<img width="1096" height="123" alt="image" src="https://github.com/user-attachments/assets/3e7f361e-09d4-4689-b198-868d5c418d94" />


## Первая сборка образа

<img width="1983" height="309" alt="image" src="https://github.com/user-attachments/assets/ec80fb0d-da3e-473b-8eb3-295193522dfe" />


## Инициализация и запуск кластера

<img width="947" height="204" alt="image" src="https://github.com/user-attachments/assets/6eb9036b-758a-4190-942f-e91a56e477e8" />

<img width="852" height="191" alt="image" src="https://github.com/user-attachments/assets/beb99526-bfd2-4d22-8371-65aba125079d" />


## Проверка работоспособности Airflow

<img width="2317" height="554" alt="image" src="https://github.com/user-attachments/assets/3fd537ca-24aa-4fcf-94c6-17438edb51fc" />


## Изменение файлов для индивидуального задания

### Изменение файла DAG (создал новый dag - **dav_var19_61.py**)

```css
Подзадача 1. Сущности (TARGET_PAGES)
Мессенджеры: WhatsApp, Telegram, WeChat.
```

<details><summary>Код *кликабельно*</summary>

```python
import urllib.request
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

TARGET_PAGES = {"WhatsApp", "Telegram", "WeChat"}

dag = DAG(
    dag_id="dag_var19_61",
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
    max_active_runs=1,
    catchup=True
)

# ...
```

</details>

### Изменение файла **streamlit/app.py**

```css
Подзадача 2. Аналитический SQL-запрос (для отчета)
Вывести средние просмотры Telegram.
```

<details><summary>Код *кликабельно*</summary>

```python
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import psycopg2

# Настройка страницы
st.set_page_config(page_title="StockSense BI Dashboard", layout="wide")

st.title("Бизнес-кейс «StockSense». Аналитика")

# Путь к CSV (общая папка data, примонтированная в Docker)
CSV_PATH = "/opt/airflow/data/pageview_data.csv"

# Функция для выполнения SQL запроса к PostgreSQL
def execute_sql_query(query):
    try:
        conn = psycopg2.connect(
            host="wiki_results",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow"
        )
        result = pd.read_sql(query, conn)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Ошибка подключения к БД: {e}")
        return None

# Загрузка данных из CSV
def load_data():
    if not os.path.exists(CSV_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(CSV_PATH)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    except Exception as e:
        st.error(f"Ошибка чтения CSV: {e}")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    st.success(f"Данные загружены (строк: {len(df)})")

    # Убираем дубликаты если есть
    df = df.drop_duplicates()

    # --- Отображение средних просмотров Telegram ---
    st.subheader("Подзадача 2. Аналитический SQL запрос")
    
    query = "SELECT AVG(pageviewcount) as avg_telegram_views FROM pageview_counts WHERE pagename = 'Telegram';"
    result_df = execute_sql_query(query)
    
    if result_df is not None and not result_df.empty:
        avg_views = result_df['avg_telegram_views'].iloc[0]

        st.metric(
            label="Средние просмотры Telegram", 
            value=f"{avg_views:,.0f}"
        )
        
        with st.expander("Показать SQL запрос и детали"):
            st.code(query, language="sql")
            st.dataframe(result_df, use_container_width=True)
    else:
        st.warning("Нет данных для Telegram. Запустите DAG и дождитесь загрузки данных.")

    # --- Столбчатая диаграмма (вариант 19) ---
    st.subheader("Подзадача 3. Столбчатая диаграмма. Просмотры по мессенджерам")
    bar_df = df.groupby('pagename')['pageviewcount'].sum().reset_index()
    fig_bar = px.bar(
        bar_df,
        x='pagename',
        y='pageviewcount',
        color='pagename',
        text='pageviewcount',
        labels={'pagename': 'Мессенджер', 'pageviewcount': 'Просмотры'},
        title='Суммарные просмотры Wikipedia по мессенджерам (WhatsApp, Telegram, WeChat)'
    )
    fig_bar.update_traces(textposition='outside')
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- Круговая диаграмма ---
    st.subheader("Круговая диаграмма. Доля просмотров")
    fig_pie = px.pie(
        bar_df,
        values='pageviewcount',
        names='pagename',
        title='Доля просмотров каждого мессенджера',
        hole=0.3
    )
    fig_pie.update_traces(textinfo='label+percent+value')
    st.plotly_chart(fig_pie, use_container_width=True)

    # --- Таблица ---
    st.subheader("Детальные данные из DWH")
    st.dataframe(df, use_container_width=True)

else:
    st.warning("Файл данных не найден. Запустите DAG в Airflow и дождитесь завершения всех задач.")
    if st.button("Обновить страницу"):
        st.rerun()
```

</details>


### Создание нового файла **scripts/avg_telegram_views.sql**

<details><summary>Код *кликабельно*</summary>

```sql
-- Вывести средние просмотры Telegram

SELECT AVG(pageviewcount) as avg_telegram_views
FROM pageview_counts 
WHERE pagename = 'Telegram';
```

</details>


## Стримлит

<img width="2275" height="958" alt="image" src="https://github.com/user-attachments/assets/67ae2f01-4890-4a73-aab5-a5bf50b82d49" />

<img width="2290" height="309" alt="image" src="https://github.com/user-attachments/assets/8e98b243-c0a8-4343-b985-29ccad049966" />


## Файлы лабораторной работы

[app.py](/61_app.py)

[dag_var19_61.py](/dag_var19_61.py)
