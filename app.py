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