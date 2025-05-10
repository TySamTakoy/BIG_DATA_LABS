from pyspark.sql import SparkSession
from pyspark import StorageLevel
from typing import List, Tuple, Dict, Set
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import requests
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

socket.setdefaulttimeout(200)


def extract_links_safe(url: str, level: int, max_level: int) -> Tuple[str, List[str]]:
    try:
        if level > max_level:
            return url, []

        print(f"Извлекаем: {url} (Уровень {level})")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []

        for link in soup.find_all('a', href=True):
            absolute_url = urljoin(url, link['href'])
            if absolute_url.startswith('http'):
                links.append(absolute_url)

        return url, links
    except Exception as e:
        print(f"Ошибка получения {url}: {e}")
        return url, []


def crawl_all_links(start_urls: List[str], max_level: int = 2, max_workers: int = 10) -> Dict[str, List[str]]:
    visited: Set[str] = set()
    links_map: Dict[str, List[str]] = {}
    executor = ThreadPoolExecutor(max_workers=max_workers)

    def crawl(url: str, level: int):
        if level > max_level or url in visited:
            return []
        visited.add(url)
        url, links = extract_links_safe(url, level, max_level)
        links_map[url] = links
        return [(link, level + 1) for link in links if link not in visited]

    queue = [(url, 1) for url in start_urls]

    while queue:
        futures = [executor.submit(crawl, url, level) for url, level in queue]
        queue = []

        for future in as_completed(futures):
            try:
                new_links = future.result()
                queue.extend(new_links)
            except Exception as e:
                print(f"Ошибка потока: {e}")

    executor.shutdown()
    return links_map


def compute_pagerank(spark: SparkSession,
                     urls: List[str],
                     iterations: int = 5,
                     max_level: int = 2) -> List[Tuple[str, float]]:

    sc = spark.sparkContext
    print("Фаза 1: Глобальный сбор ссылок")
    links_map = crawl_all_links(urls, max_level=max_level)

    links_rdd = sc.parallelize(links_map.items())
    all_pages = links_rdd.flatMap(lambda x: [x[0]] + x[1]).distinct().persist(StorageLevel.MEMORY_AND_DISK)

    graph = links_rdd.flatMap(lambda x: [(x[0], to) for to in x[1]]) \
                     .groupByKey() \
                     .mapValues(list) \
                     .rightOuterJoin(all_pages.map(lambda x: (x, []))) \
                     .mapValues(lambda x: x[0] if x[0] is not None else []) \
                     .persist(StorageLevel.MEMORY_AND_DISK)

    total_pages = all_pages.count()
    ranks = all_pages.map(lambda x: (x, 1.0)).persist(StorageLevel.MEMORY_AND_DISK)

    print("Фаза 2: Итерации PageRank")
    for i in range(iterations):
        print(f"Итерация {i+1}/{iterations}")

        joined = graph.join(ranks)

        contribs = joined.flatMap(lambda x:
            [(dest, x[1][1] / len(x[1][0])) for dest in x[1][0]] if x[1][0] else [])

        dangling_mass = joined.filter(lambda x: not x[1][0]).map(lambda x: x[1][1]).sum()

        new_ranks = contribs.reduceByKey(lambda x, y: x + y) \
                            .mapValues(lambda x: 0.15 + 0.85 * x) \
                            .rightOuterJoin(all_pages.map(lambda x: (x, None))) \
                            .mapValues(lambda x: x[0] if x[0] is not None else 0.15) \
                            .mapValues(lambda rank: rank + 0.85 * dangling_mass / total_pages) \
                            .persist(StorageLevel.MEMORY_AND_DISK)

        delta = ranks.join(new_ranks).map(lambda x: abs(x[1][0] - x[1][1])).sum()
        print(f"Среднее изменение ранга: {delta:.6f}")

        ranks.unpersist()
        ranks = new_ranks

    print("Фаза 3: Сбор результатов")
    results = ranks.sortBy(lambda x: -x[1]).take(20)  # заменено collect()

    graph.unpersist()
    all_pages.unpersist()
    links_rdd.unpersist()

    return results


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StablePageRank") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.network.timeout", "600s") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    test_urls = [
        "https://ya.ru",
        "https://vk.com",
        "https://habr.com",
        "https://github.com"
    ]

    print("Запуск вычисления PageRank...")
    start_time = time.time()

    try:
        results = compute_pagerank(spark, test_urls, iterations=5, max_level=2)

        print("\nТоп результатов PageRank:")
        for url, rank in results:
            print(f"{url[:70]}...: {rank:.4f}")

    except Exception as e:
        print(f"Ошибка расчёта: {str(e)}")
    finally:
        print(f"\nОбщее время выполнения: {time.time() - start_time:.2f} секунд")
        spark.stop()
