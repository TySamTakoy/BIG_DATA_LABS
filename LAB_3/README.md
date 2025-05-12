# LAB_3

# РЕШЕНИЕ в Google Collab - https://colab.research.google.com/drive/14_7B-xBjW7ZkSq9oSJ_AVuPziG4WO2Zu?usp=sharing

# Видео пояснение - https://drive.google.com/file/d/1Z0sgNEEEa5PAL561JKiTqpb7PaBjoKTu/view?usp=drive_link

Реализация алгоритма **PageRank** с использованием **Apache Spark** и многопоточного краулера на Python. 

Начав с заданных URL, система строит граф переходов между страницами и вычисляет их ранги.

##  Возможности

- **Многопоточный веб-краулер** с контролем глубины
- **Распределённые вычисления PageRank** на PySpark
- Вывод **топ-20 URL** по значению PageRank
- Настройка количества итераций, глубины обхода и числа потоков

## Настройка

Параметры можно изменить напрямую в коде:

```
test_urls = [
    "https://ya.ru",
    "https://vk.com",
    "https://habr.com",
    "https://github.com"
]

iterations = 5        # Количество итераций PageRank
max_level = 2         # Глубина обхода ссылок
max_workers = 10      # Количество потоков для краулера
```

## Настройка spark

```
SparkSession.builder \
    .appName("StablePageRank") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.network.timeout", "600s")
```

## Пример вывода

```
Запуск вычисления PageRank...
Фаза 1: Глобальный сбор ссылок
Извлекаем: https://ya.ru (Уровень 1)

Фаза 2: Итерации PageRank
Итерация 1/5
Среднее изменение ранга: 0.231244


Топ результатов PageRank:
https://github.com...: 0.2713
https://ya.ru...: 0.1962
```
