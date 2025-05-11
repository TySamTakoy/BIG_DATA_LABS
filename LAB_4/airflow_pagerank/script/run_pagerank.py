import os
import time
import csv
import logging
import socket
import requests
from typing import List, Tuple, Dict, Set
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark import StorageLevel

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
socket.setdefaulttimeout(200)


def extract_links_safe(url: str, level: int, max_level: int) -> Tuple[str, List[str]]:
    try:
        if level > max_level:
            return url, []
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for link in soup.find_all('a', href=True):
            absolute_url = urljoin(url, link['href'])
            if absolute_url.startswith('http'):
                links.append(absolute_url)
        return url, links
    except Exception as e:
        log.warning(f"Ошибка при обработке {url}: {e}")
        return url, []


def crawl_all_links(start_urls: List[str], max_level: int = 2, max_workers: int = 10) -> Dict[str, List[str]]:
    visited: Set[str] = set()
    links_map: Dict[str, List[str]] = {}

    def crawl(url: str, level: int):
        if level > max_level or url in visited:
            return []
        visited.add(url)
        url, links = extract_links_safe(url, level, max_level)
        links_map[url] = links
        return [(link, level + 1) for link in links if link not in visited]

    queue = [(url, 1) for url in start_urls]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while queue:
            futures = [executor.submit(crawl, url, level) for url, level in queue]
            queue = []
            for future in as_completed(futures):
                try:
                    new_links = future.result()
                    queue.extend(new_links)
                except Exception as e:
                    log.warning(f"Ошибка при выполнении crawl: {e}")
    return links_map


def compute_pagerank(spark: SparkSession, urls: List[str], iterations: int = 5, max_level: int = 2) -> List[Tuple[str, float]]:
    sc = spark.sparkContext
    links_map = crawl_all_links(urls, max_level=max_level)

    links_rdd = sc.parallelize(links_map.items())
    all_pages = links_rdd.flatMap(lambda x: [x[0]] + x[1]).distinct().persist(StorageLevel.MEMORY_AND_DISK)

    graph = links_rdd.flatMap(lambda x: [(x[0], to) for to in x[1]]) \
                     .groupByKey().mapValues(list) \
                     .rightOuterJoin(all_pages.map(lambda x: (x, []))) \
                     .mapValues(lambda x: x[0] if x[0] is not None else []) \
                     .persist(StorageLevel.MEMORY_AND_DISK)

    total_pages = all_pages.count()
    ranks = all_pages.map(lambda x: (x, 1.0)).persist(StorageLevel.MEMORY_AND_DISK)

    for i in range(iterations):
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
        ranks.unpersist()
        ranks = new_ranks

    results = ranks.sortBy(lambda x: -x[1]).take(20)

    graph.unpersist()
    all_pages.unpersist()
    links_rdd.unpersist()
    ranks.unpersist()

    return results


def save_to_csv(results, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['URL', 'PageRank'])
        writer.writerows(results)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PageRankPipeline") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    test_urls = [
        "https://ya.ru",
        "https://habr.com",
        "https://github.com"
    ]

    start = time.time()
    try:
        results = compute_pagerank(spark, test_urls, iterations=5)
        save_to_csv(results, "/opt/airflow/data/pagerank_results.csv")
        log.info("PageRank успешно рассчитан и сохранён.")
    except Exception as e:
        log.error("Ошибка во время выполнения PageRank:", exc_info=True)
    finally:
        log.info(f"Время выполнения: {time.time() - start:.2f} секунд")
        spark.stop()
