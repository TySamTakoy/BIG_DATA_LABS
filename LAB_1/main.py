# Импорт библиотек
import csv
from collections import defaultdict

# Функция чтения данных
def read_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader)

# ----------------------------------------------------------------------
# Сравнение среднего размера мошеннических и обычных транзакций
# ----------------------------------------------------------------------

# Функция mapper
def mapper_1(transaction):
    #проверка транзакции на мошенничество
    is_fraud = transaction['isFraud'] == '1'
    amount = float(transaction['amount'])
    yield ('fraud_amount' if is_fraud else 'normal_amount', amount)

# Функция shuffle
def shuffle(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled

# Функция reducer
def reducer_1(shuffled_data):
    results = {}
    for key in ['fraud_amount', 'normal_amount']:
        if key in shuffled_data:
            values = shuffled_data[key]
            results[key] = sum(values) / len(values)
    return results

# ----------------------------------------------------------------------
# Распределение мошеннических транзакций по типам операций
# ----------------------------------------------------------------------

# функция mapper
def mapper_2(transaction):
    #проверка транзакции на мошенничество
    is_fraud = transaction['isFraud'] == '1'
    if is_fraud:
        transaction_type = transaction['type']
        yield (f'fraud_type_{transaction_type}', 1)

# Функция shuffle из задачи сравнения среднего размера мошеннических и обычных транзакций
# Полностью совпадает с необходимой функцией shuffle для задачи распределения
# Мошеннических транзакций по типам операций

# Функция reducer
def reducer_2(shuffled_data):
    results = {}
    type_counts = {}

    for key in shuffled_data:
        if key.startswith('fraud_type_'):
            transaction_type = key[11:]
            type_counts[transaction_type] = sum(shuffled_data[key])

    if type_counts:
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        results['fraud_types_ranking'] = sorted_types

    return results

# Функция main
def main(file_path):
    #данные из файла
    data = read_data(file_path)

    mapped_values_1 = []
    mapped_values_2 = []

    for transaction in data:
        for output in mapper_1(transaction):
            mapped_values_1.append(output)
        for output in mapper_2(transaction):
            mapped_values_2.append(output)

    shuffled_data_1 = shuffle(mapped_values_1)
    shuffled_data_2 = shuffle(mapped_values_2)

    results_1 = reducer_1(shuffled_data_1)
    results_2 = reducer_2(shuffled_data_2)


    #вывод результата по среднему размеру мошеннических и обычных транзакций
    print("-----------------------------------------------------------------------------")
    print("Средний размер транзакций, которые были отмечены как:")
    print(f"-мошеннические: {results_1.get('fraud_amount', 0):.2f}")
    print(f"-обычные: {results_1.get('normal_amount', 0):.2f}")
    print("-----------------------------------------------------------------------------\n")

    print("-----------------------------------------------------------------------------")
    print("Распределение мошеннических транзакций по типам операций:")
    for i, (t_type, count) in enumerate(results_2.get('fraud_types_ranking', []), 1):
        print(f"{i}. {t_type}: {count} транзакция(-ий)")
    print("-----------------------------------------------------------------------------")

if __name__ == '__main__':
    file_path = 'onlinefraud.csv'
    main(file_path)