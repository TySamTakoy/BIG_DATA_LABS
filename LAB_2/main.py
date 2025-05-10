!apt-get install openjdk-17-jdk
!wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
!tar fx hadoop-3.4.1.tar.gz


import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["HADOOP_HOME"] = "/content/hadoop-3.4.1"


!ln -sf /content/hadoop-3.4.1/bin/* /usr/bin
!hadoop version
!hdfs dfs -ls -h -R     # Recursively list subdirectories with human-readable file sizes.

!pip install snakebite
!pip install snakebite-py3

!hdfs dfs -ls
!pip install hdfs


from google.colab import drive
drive.mount('/content/drive')


ls /content/drive/MyDrive/'Colab Notebooks'/onlinefraud.csv

!hdfs dfs -put /content/drive/MyDrive/'Colab Notebooks'/onlinefraud.csv /
!hdfs dfs -ls

!head -n 100 "/content/drive/MyDrive/Colab Notebooks/onlinefraud.csv"

!ls "/content/drive/MyDrive/Colab Notebooks/onlinefraud.csv"



%%writefile mapper_1.py
#!/usr/bin/env python

import sys
import csv

lines = sys.stdin.readlines()

if not lines:
    sys.exit()

header = lines[0].strip().split(',')

for line in lines[1:]:
    line = line.strip()
    reader = csv.DictReader([line], fieldnames=header)
    for transaction in reader:
        try:
            is_fraud = transaction['isFraud'] == '1'
            amount = float(transaction['amount'])
            key = 'fraud_amount' if is_fraud else 'normal_amount'
            print(f'{key}\t{amount}')
        except:
            continue

# Writing mapper_1.py

!python mapper_1.py < "/content/drive/MyDrive/Colab Notebooks/onlinefraud.csv"



%%writefile reducer_1.py
#!/usr/bin/env python
from collections import defaultdict
import sys

# Функция shuffle
def shuffle(mapped_values):
    shuffled = defaultdict(list)
    for line in mapped_values:
        # Разделяем по табуляции, как в выводе маппера
        key, value = line.strip().split('\t', 1)

        # Преобразуем строку value в float
        try:
            value = float(value)
        except ValueError:
            continue  # Если значение не преобразуется, пропускаем эту строку

        # Группируем по ключу
        shuffled[key].append(value)
    return shuffled

# Чтение данных из stdin (по строкам)
mapped_values = sys.stdin.readlines()

# Вызов shuffle для группировки значений
shuffled_data = shuffle(mapped_values)

# Функция reducer
def reducer_1(shuffled_data):
    results = {}
    # Агрегируем значения для ключей 'fraud_amount' и 'normal_amount'
    for key in ['fraud_amount', 'normal_amount']:
        if key in shuffled_data:
            values = shuffled_data[key]
            # Вычисляем среднее значение
            results[key] = sum(values) / len(values)
    return results

# Получаем результаты редьюсера
results = reducer_1(shuffled_data)

# Выводим результаты
for key, value in results.items():
    print(f'{key}\t{value}')

# Writing reducer_1.py

!cat "/content/drive/MyDrive/Colab Notebooks/onlinefraud.csv" | python mapper_1.py | sort | python reducer_1.py | sort -r

# normal_amount	178197.04172739814
# fraud_amount	1467967.2991403856


%%writefile mapper_2.py
#!/usr/bin/env python

import sys
import csv

# Читаем данные из stdin
lines = sys.stdin.readlines()

if not lines:
    sys.exit()

# Получаем заголовки
header = lines[0].strip().split(',')

# Обрабатываем каждую строку начиная со второй
for line in lines[1:]:
    line = line.strip()
    if not line:
        continue
    reader = csv.DictReader([line], fieldnames=header)
    for transaction in reader:
        try:
            is_fraud = transaction['isFraud'] == '1'
            if is_fraud:
                transaction_type = transaction['type']
                print(f'fraud_type_{transaction_type}\t1')
        except:
            continue

# Writing mapper_2.py

!python mapper_2.py < "/content/drive/MyDrive/Colab Notebooks/onlinefraud.csv"


%%writefile reducer_2.py
#!/usr/bin/env python
import sys
from collections import defaultdict

# Функция shuffle: группировка значений по ключу
def shuffle(mapped_lines):
    shuffled = defaultdict(list)
    for line in mapped_lines:
        try:
            key, value = line.strip().split('\t', 1)
            value = int(value)
            shuffled[key].append(value)
        except:
            continue
    return shuffled

# Функция reducer: подсчет количества по типам fraud_type
def reducer_2(shuffled_data):
    results = {}
    type_counts = {}

    for key in shuffled_data:
        if key.startswith('fraud_type_'):
            transaction_type = key[len('fraud_type_'):]
            type_counts[transaction_type] = sum(shuffled_data[key])

    if type_counts:
        # Сортируем по убыванию количества
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        results['fraud_types_ranking'] = sorted_types

    return results

# Чтение входных данных
mapped_lines = sys.stdin.readlines()
shuffled_data = shuffle(mapped_lines)
results = reducer_2(shuffled_data)

# Вывод результатов
for transaction_type, count in results.get('fraud_types_ranking', []):
    print(f'{transaction_type}\t{count}')

# Writing reducer_2.py

!cat "/content/drive/MyDrive/Colab Notebooks/onlinefraud.csv" | python mapper_2.py | sort | python reducer_2.py | sort -r

# TRANSFER	4097
# CASH_OUT	4116
