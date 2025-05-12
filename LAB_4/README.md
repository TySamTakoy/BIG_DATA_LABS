# LAB_4

# Видео пояснение - https://drive.google.com/file/d/1lFl6cPLC1yeHijvos--G4v9bKz_6CSjA/view?usp=drive_link

# Использование витрины данных Apache Airflow для формирования цепочки заданий для периодического запуска PageRank (из LAB_3) и представление данных в удобном для пользователя виде (csv & html)

## Структура проекта

```
airflow_pagerank/
├── dags/
│   └── pagerank_dag.py          # Основной DAG для вычисления PageRank
├── data/
│   ├── pagerank_results.csv     # Результаты PageRank в формате CSV
│   └── pagerank_results.html    # Результаты PageRank в формате HTML
├── script/
│   ├── csv_to_html.py           # Скрипт для конвертации CSV в HTML
│   └── run_pagerank.py          # Скрипт для вычисления PageRank с использованием Spark
├── Dockerfile                   # Dockerfile для сборки контейнера Airflow
├── docker-compose.yml           # Конфигурация для развертывания проекта с использованием Docker Compose
└── requirements.txt             # Необходимые зависимости для проекта
```

## Описание файлов

### ```dags/pagerank_dag.py```

Этот файл описывает DAG (Directed Acyclic Graph) в Apache Airflow для вычисления PageRank. Он выполняет следующие шаги:

1. Запускает DAG с сообщением о старте.

2. Выполняет вычисление PageRank с использованием скрипта ```run_pagerank.py```.

3. Конвертирует результаты из CSV в HTML с помощью скрипта ```csv_to_html.py```.

4. Завершается с сообщением о завершении работы.

### ```script/run_pagerank.py```

Этот файл отвечает за вычисление алгоритма PageRank. Он выполняет следующие шаги:

1. Скачивает веб-страницы и извлекает ссылки на другие страницы с использованием библиотеки BeautifulSoup.

2. Строит граф ссылок между страницами.

3. Вычисляет PageRank с использованием Apache Spark.

4. Сохраняет результаты в CSV файл.

### ```script/csv_to_html.py```

Этот скрипт конвертирует результаты из CSV в HTML для удобного просмотра.

### ```docker-compose.yml```

Конфигурация для развертывания всех необходимых сервисов (PostgreSQL, Airflow, Spark) с использованием Docker Compose. Включает:

1. PostgreSQL для хранения данных Airflow.

2. Airflow для оркестрации DAG.

3. Apache Spark для выполнения вычислений PageRank.

### ```Dockerfile```

Cборкf контейнера Airflow с необходимыми зависимостями и инициализацией базы данных.

### ```requirements.txt```
Файл зависимостей для проекта, который включает библиотеки, необходимые для работы с Airflow, Spark и другими инструментами.

## Запуск проекта

### Сборка и запуск контейнеров

1. Docker и Docker Compose

2. В корневой папке проекта

3. ```docker-compose build```

4. ```docker-compose up -d```

### Airflow Web UI

Доступ по адресу ```http://localhost:8080```

Стандартные учетные данные (admin):

```
Username: airflow
Password: airflow
```
Внешний вид интерфейса:

![image](https://github.com/user-attachments/assets/f2415ce3-69ca-4ced-b366-c529ecb9c0e9)

Запуск DAG через кнопку Trigger DAG

![image](https://github.com/user-attachments/assets/fb3f7b1f-dade-4f7a-b285-8b1211a4c791)

pagerank_pipeline в виде графа

![image](https://github.com/user-attachments/assets/8ba36d93-5154-4c3b-8c08-04308225a354)

### Вывод результатов (топ 20 результатов)

Результаты вычислений PageRank будут сохранены в файле ```pagerank_results.csv``` и преобразованы в ```pagerank_results.html```. Оба файла будут расположены в папке ```data/```.

Пример вывода результатов в формате ```.csv```

![image](https://github.com/user-attachments/assets/886c4105-2d21-49b5-861f-4e68c338e1a8)

Пример вывода в результатов в формате ```.html```

![image](https://github.com/user-attachments/assets/346e990c-7856-4114-b7e2-d575bcc30759)

