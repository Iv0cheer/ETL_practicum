<img width="701" height="360" alt="image" src="https://github.com/user-attachments/assets/5c40468e-79be-477c-9f57-98550dc9de93" />### 
## **Вариант №19**

Предметная область: `Управление проектами.`

> PostgreSQL: Проекты
> Excel: Ресурсы и бюджеты.
> CSV: Отчеты о выполнении (таймшиты).

> Мониторинг проектов. Сравнить плановый бюджет с фактическими затратами времени сотрудников.


### Создание трансформаций:


<details><summary> **Файл *.csv в MySQL** </summary>

  Общая структура:
  
  <img width="853" height="402" alt="image" src="https://github.com/user-attachments/assets/2d560a3a-52e3-426c-979d-e8979c1b6b3e" />

  Переименование таблиц:

  <img width="1624" height="526" alt="image" src="https://github.com/user-attachments/assets/876e75f7-21c1-4fbd-ac20-6f1a77cd589a" />

  Шаг с загрузкой в MySQL:

  <img width="953" height="873" alt="image" src="https://github.com/user-attachments/assets/b7c2936b-698d-41f1-98e5-2868c8b5bb78" />

</details>

<details><summary> **Файл *.xlsx в MySQL** </summary>

  Общая структура:

  <img width="701" height="360" alt="image" src="https://github.com/user-attachments/assets/d1d4d328-ebe0-4737-9b7d-f048569c5331" />

  Выбор и переименование полей:

  <img width="1563" height="903" alt="image" src="https://github.com/user-attachments/assets/13208f7a-1fe7-4daa-8d38-280b54335cf7" />

  Загрузка в MySQL:

  <img width="948" height="861" alt="image" src="https://github.com/user-attachments/assets/febe32fe-42c8-4a1c-8934-423068855f0e" />

</details>


<details><summary> **PostgreSQL в MySQL** </summary>

Подключение к PostgreSQL

<img width="1033" height="709" alt="image" src="https://github.com/user-attachments/assets/bba7524a-a5eb-4d68-9456-c3afb6b07160" />

Выбор колонок:

<img width="425" height="284" alt="image" src="https://github.com/user-attachments/assets/0295b9d8-8b5c-45ea-8167-52510ff09e58" />

Последующая загрузка в MySQL:

<img width="948" height="865" alt="image" src="https://github.com/user-attachments/assets/75fff42a-f772-4681-a0d0-cacad7a9748a" />

</details>

### Создание Оркестрации:

Общая структура:

<img width="1059" height="424" alt="image" src="https://github.com/user-attachments/assets/fdcc88ac-bdd7-4126-a1ce-1e521989cbe2" />

Изменение ссылки RAW в GIT:

<img width="1547" height="895" alt="image" src="https://github.com/user-attachments/assets/3d4a0d4f-0984-4cc2-aae6-712e404097aa" />

Запуск оркестрации и ее отработка:

<img width="1967" height="1083" alt="image" src="https://github.com/user-attachments/assets/b45464d6-47b5-4760-84ce-f8ca171ee638" />


Проверка таблиц в phpMyAdmin:

<details><summary>**Таблица projects**</summary>

<img width="856" height="441" alt="image" src="https://github.com/user-attachments/assets/7c404795-86c5-4818-b1ea-c9b72cfbc3fd" />

</details>

<details><summary>**Таблица proj_timesheets**</summary>

<img width="958" height="816" alt="image" src="https://github.com/user-attachments/assets/ba1e8be1-80b9-4d05-bf03-923053482be2" />

</details>

<details><summary>**Таблица resources_pl**</summary>

<img width="1932" height="451" alt="image" src="https://github.com/user-attachments/assets/7873cac8-cf45-4384-9334-79a394da0cbf" />

</details>


## Приложения к лабораторной работе:

[Файл трансформации CSV --> MySQl]()

[Файл трансформации Excel --> MySQl]()

[Файл трансформации PostgreSQL --> MySQl]()

[Файл Job]()
