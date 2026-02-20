## **Вариант №19**

**Основной фильтр для загрузки в БД**
> Маржа (Profit/Sales) > 0.2

**Доп. задание 1 (Аналитика)**
> Анализ по штатам

**Доп. задание 2 (Аналитика)**
> Отчет по категориям

### Реализация основного фильтра для загрузки в БД

* Трансформация lab_02_1_csv_orders

Общая структура трансформации

<img width="200" alt="image" src="https://github.com/user-attachments/assets/54044983-fe63-47dc-aa5e-2c9c8347bfd8" />


Добавил вычисляемое поле маржи

<img width="1565" height="626" alt="image" src="https://github.com/user-attachments/assets/de22cc50-bddf-4497-97e7-793ffabd5539" />


Добавил условие (фильтр) для загрузки данных в БД

<img width="1054" height="569" alt="image" src="https://github.com/user-attachments/assets/c83c0a62-5601-4b13-9fd6-0d439271026b" />

Дальше почистил NULL в таблице

<img width="1109" height="366" alt="image" src="https://github.com/user-attachments/assets/68840e6b-54cf-49f6-8003-50832fb46531" />


#### Job и его запуск

Скриншот общей структуры

<img width="1443" height="1079" alt="image" src="https://github.com/user-attachments/assets/2c7383be-720a-4dc2-b0e3-c9e723d1a403" />


Скриншот модуля HTTP

<img width="1540" height="890" alt="image" src="https://github.com/user-attachments/assets/db193481-31f8-4dc3-a721-fe176b994b56" />


### Доп. Задание 1 — Анализ по штатам

Результат анализа:
<img width="1822" height="1146" alt="image" src="https://github.com/user-attachments/assets/bfda9fcc-0ba0-4ae3-8f91-55063009b38a" />

Выводы:
Ключевые рынки: Штаты Нью-Йорк и Калифорния являются абсолютными лидерами по объему выручки (210 тыс. и 168 тыс. долларов соответственно).
Это ключевые регионы для бизнеса с массой клиентов.

Между лидерами и остальными штатами есть большой разрыв.


### Доп. задание 2 — Отчет по категориям

Результат анализа:

<img width="1352" height="570" alt="image" src="https://github.com/user-attachments/assets/40fa4810-d277-4029-9569-483d586c2318" />

<img width="1352" height="599" alt="image" src="https://github.com/user-attachments/assets/f9675130-8c49-4d79-9ba5-b2ca919c3edc" />



> Основную долю выручки приносят Офисные принадлежности (Office Supplies) — 42,2% и Технологии (Technology) — 40,7%. Мебель (Furniture) занимает лишь 17,1%. Это говорит о том, что чаще всего покупают либо расходные материалы для офиса, либо дорогостоящую технику.

> Основная категория покупок - технологии. Они генерирует наибольший объем выручки, наибольшее количество заказов и продаж, в особенности в штатах Нью-Йорк и Калифорния


### Основные выводы по обоим блокам:
Стоит сосредоточить маркетинговые усилия на продвижении Технологий в Нью-Йорке и Калифорнии.



## Приложения к лабораторной работе:

[Файл анализа Colaboratory](https://colab.research.google.com/drive/1FB2esOXDnDtEfBRJjTzSw9UamhTW1eb9?usp=sharing)

[Файл lab_02_1_csv_orders.ktr](/lab_02_1_csv_orders.ktr)

[Файл lab_02_2_csv_to_Customers.ktr](/lab_02_2_csv_to_Customers.ktr)

[Файл lab_02_3_csv_to_products.ktr](/lab_02_3_csv_to_products.ktr)

[Файл Job_CSV_to_MYsql.kjb](/Job_CSV_to_MYsql.kjb)
