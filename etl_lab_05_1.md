# Лабораторная работа №5.1
## 
### Вариант 19.

| Параметр | Значение |
|----------|---------|
| Город | Вена |
| Период прогноза | 5 дней |
| Фильтр | Только выходные дни |
| Расчет | Средняя температура выходных |

---

## Цель работы



---

Сбор Docker образа

<img width="1706" height="276" alt="image" src="https://github.com/user-attachments/assets/0e4929f5-a1a3-493a-80b2-021bf1e4f434" />

Запуск сервисов

<img width="1312" height="1023" alt="image" src="https://github.com/user-attachments/assets/9cc0ebe5-8b49-4ea6-9c8e-e96b980eeeaa" />

---

## Написание DAG

<details><summary>Код для DAG (variant_19.py)</summary>

```python
import os
import requests
import pandas as pd
import joblib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sklearn.linear_model import LinearRegression

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id="variant_19_vienna_weekend",
    default_args=default_args,
    description="Vienna weather forecast 5 days, filter weekends, average temp for ML",
    schedule_interval="@daily",
    catchup=False
)


def ensure_data_dir():
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def fetch_weather_forecast():
    """
    Получение прогноза погоды для Вены на 5 дней через WeatherAPI
    """
    data_dir = ensure_data_dir()
    
    api_key = "cc218e2a33be41c9b5b185618260104"
    
    url = (
        f"http://api.weatherapi.com/v1/forecast.json"
        f"?key={api_key}"
        f"&q=48.2082,16.3738"
        f"&days=5"
        f"&aqi=no"
        f"&alerts=no"
    )
    
    response = requests.get(url)
    data = response.json()
    
    dates = []
    temperatures = []
    
    for forecast_day in data['forecast']['forecastday']:
        dates.append(forecast_day['date'])
        temperatures.append(forecast_day['day']['avgtemp_c'])
    
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    df['date'] = pd.to_datetime(df['date'])
    df['weekday'] = df['date'].dt.day_name()
    df['is_weekend'] = df['weekday'].isin(['Saturday', 'Sunday'])
    
    file_path = os.path.join(data_dir, 'weather_forecast_vienna.csv')
    df.to_csv(file_path, index=False)
    
    print(f"Weather forecast for Vienna (5 days) saved.")
    print(f"Weekend days: {df[df['is_weekend']]['date'].dt.date.tolist()}")

def filter_weekend_and_calc_avg_temp():
    """
    Фильтрация только выходные дни + ср темпа
    Вариант 19
    """
    data_dir = ensure_data_dir()
    
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast_vienna.csv'))
    
    df['date'] = pd.to_datetime(df['date'])
    df['weekday'] = df['date'].dt.day_name()
    
    weekend_days = df[df['weekday'].isin(['Saturday', 'Sunday'])]
    
    if len(weekend_days) == 0:
        raise ValueError(f"В 5-дневном прогнозе для Вены нет выходных дней! "
                         f"Дни в прогнозе: {df['date'].dt.date.tolist()}")
    
    avg_temp = weekend_days['temperature'].mean()
    
    
    result_df = pd.DataFrame({
        'weekend_dates': [weekend_days['date'].dt.date.tolist()],
        'weekend_temperatures': [weekend_days['temperature'].tolist()],
        'avg_temperature_weekend': [avg_temp],
        'calculation_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
    })
    
    file_path = os.path.join(data_dir, 'weekend_avg_temp.csv')
    result_df.to_csv(file_path, index=False)
    
    print(f"\n-----")
    print(f"РЕЗУЛЬТАТ ФИЛЬТРАЦИИ (Вариант 19)")
    print(f"Выходные дни в прогнозе:")
    for idx, row in weekend_days.iterrows():
        print(f"  {row['date'].date()}: {row['temperature']}°C")
    print(f"\nСредняя температура выходных: {avg_temp:.2f}°C")
    print(f"-----")

def fetch_sales_data():
    """
    Генерация данных продаж (для обучения модели)
    """
    
    data_dir = ensure_data_dir()
    
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast_vienna.csv'))
    dates = weather_df['date'].tolist()
    
    temps = weather_df['temperature'].tolist()
    sales = []
    for temp in temps:
        import random
        sale = 10 + (temp * 2) + random.randint(-2, 2)
        sales.append(max(0, sale))
    
    df = pd.DataFrame({'date': dates, 'sales': sales})
    
    
    file_path = os.path.join(data_dir, 'sales_data_vienna.csv')
    df.to_csv(file_path, index=False)
    
    print("Sales data for Vienna saved.")

def join_datasets():
    """
    Объединение погодных данных с данными о продажах
    """
    
    data_dir = ensure_data_dir()
    
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast_vienna.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'sales_data_vienna.csv'))
    
    joined_df = pd.merge(weather_df, sales_df, on='date', how='inner')
    
    joined_df['temperature'] = joined_df['temperature'].fillna(joined_df['temperature'].mean())
    joined_df['sales'] = joined_df['sales'].fillna(joined_df['sales'].mean())
    
    
    file_path = os.path.join(data_dir, 'joined_data_vienna.csv')
    joined_df.to_csv(file_path, index=False)
    
    print(f"Joined dataset saved with {len(joined_df)} records.")

def train_ml_model():
    
    data_dir = ensure_data_dir()
    
    df = pd.read_csv(os.path.join(data_dir, 'joined_data_vienna.csv'))
    
    X = df[['temperature']]
    y = df['sales']
    
    model = LinearRegression()
    model.fit(X, y)
    
    
    file_path = os.path.join(data_dir, 'ml_model_vienna.pkl')
    joblib.dump(model, file_path)
    
    model_info = {
        'intercept': model.intercept_,
        'coef': model.coef_[0],
        'r2_score': model.score(X, y)
    }
    
    print(f"ML model trained and saved.")
    print(f"Model coefficients: intercept={model.intercept_:.2f}, coef={model.coef_[0]:.2f}")
    print(f"R² score: {model.score(X, y):.4f}")

def predict_sales_for_weekend():
    """
    Прогнозирование продаж на выходные (ср. темпа)
    """
    
    data_dir = ensure_data_dir()

    model = joblib.load(os.path.join(data_dir, 'ml_model_vienna.pkl'))

    weekend_data = pd.read_csv(os.path.join(data_dir, 'weekend_avg_temp.csv'))
    avg_temp = weekend_data['avg_temperature_weekend'].iloc[0]

    input_data = pd.DataFrame({'temperature': [avg_temp]})

    predicted_sales = model.predict(input_data)

    prediction_result = pd.DataFrame({
        'prediction_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        'avg_weekend_temp_c': [avg_temp],
        'predicted_sales': [predicted_sales[0]]
    })
    
    
    file_path = os.path.join(data_dir, 'weekend_sales_prediction.csv')
    prediction_result.to_csv(file_path, index=False)
    
    print(f"\n-----")
    print(f"РЕЗУЛЬТАТ ПРОГНОЗА (Вариант 19)")
    print(f"Средняя температура на выходные: {avg_temp:.2f}°C")
    print(f"Прогнозируемые продажи: {predicted_sales[0]:.2f}")
    print(f"-----")

t1 = PythonOperator(
    task_id="fetch_weather_forecast_vienna", 
    python_callable=fetch_weather_forecast, 
    dag=dag
)

t2 = PythonOperator(
    task_id="filter_weekend_and_calc_avg_temp", 
    python_callable=filter_weekend_and_calc_avg_temp, 
    dag=dag
)

t3 = PythonOperator(
    task_id="fetch_sales_data_vienna", 
    python_callable=fetch_sales_data, 
    dag=dag
)

t4 = PythonOperator(
    task_id="join_datasets_vienna", 
    python_callable=join_datasets, 
    dag=dag
)

t5 = PythonOperator(
    task_id="train_ml_model_vienna", 
    python_callable=train_ml_model, 
    dag=dag
)

t6 = PythonOperator(
    task_id="predict_sales_for_weekend", 
    python_callable=predict_sales_for_weekend, 
    dag=dag
)

t1 >> t2  # Сначала получаю погоду, затем фильтр и средняя темпа
t1 >> t3  # Для обучения - те же даты
[t2, t3] >> t4  # После получения погоды и продаж - объединяю
t4 >> t5  # Обучаю модель на t4 данных
t5 >> t6  # Делаю прогноз на основе ср темпы на выхах
```

</details>


## Выполнение DAG

### криншот успешного выполнения

<img width="2287" height="326" alt="image" src="https://github.com/user-attachments/assets/750948bb-d84b-42de-aca9-ccc6af518c34" />

Все таски выполнены успешно (зеленые все).

### Результаты

**Прогноз погоды для Вены (5 дней):**

| Дата | Температура (°C) | День недели | Выходной |
|------|-----------------|-------------|---------|
| 2026-04-01 | 6.8 | Wednesday | Нет |
| 2026-04-02 | 5.7 | Thursday | Нет |
| 2026-04-03 | 9.5 | Friday | Нет |
| 2026-04-04 | 9.6 | Saturday | **Да** |
| 2026-04-05 | 13.0 | Sunday | **Да** |

**Результат фильтрации:**

| Параметр | Значение |
|----------|---------|
| Выходные дни | 04.04.2026, 05.04.2026 |
| Температура в выходные | 9.6°C, 13.0°C |
| **Средняя температура выходных** | **11.3°C** |

**Прогноз продаж:**

| Параметр | Значение |
|----------|---------|
| Средняя температура выходных | 11.3°C |
| Прогнозируемые продажи | 32.57 |

---

## Аналитика в Jupyter Notebook

[Файл prediction_of_sales_19.ipynb](/prediction_of_sales_19.ipynb)

## Возникшие трудности и их решения

| Трудность | Как решил |
|-----------|-----------|
| Ошибка Permission denied при записи файлов в `/opt/airflow/data` | Выполнил `sudo chmod -R 777 data` на локальной машине |
| Ошибка `Object of type DataFrame is not JSON serializable` при возврате DataFrame из таска | Убрал `return df` из функций |
| В 5-дневном прогнозе могут отсутствовать выходные дни | Добавил проверку `if len(weekend_days) == 0: raise ValueError` |
