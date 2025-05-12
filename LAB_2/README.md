  # Lab_2
  
  # РЕШЕНИЕ (в Google Collab) - https://colab.research.google.com/drive/1BD_F-yAWKpeUKyGrEGeO3JUrUXpo7lOl#scrollTo=b5kMRG-JKJNN

  # Видео пояснение - https://drive.google.com/file/d/1FNO1J6A2nero1YZXrgol9vjjgVZ3ZGaJ/view?usp=drive_link

MapReduce для набора данных ('onlinefraud.csv') через Apache Hadoop

Url исходного файла: https://www.kaggle.com/datasets/jainilcoder/online-payment-fraud-detection?resource=download

![image](https://github.com/user-attachments/assets/78d66d30-12ef-44c1-95d4-621e839da787)


  ## Задачи:
1. Сравнение среднего размера мошеннических и обычных транзакций
2. Распределение мошеннических транзакций по типам операций

  ## Используемые библиотеки
```
import os
from collections import defaultdict
import sys
import csv

from google.colab import drive
drive.mount('/content/drive')
```

  ## Вывод:
```
normal_amount	178197.04172739814
fraud_amount	1467967.2991403856
```

```
TRANSFER	4097
CASH_OUT	4116
```
