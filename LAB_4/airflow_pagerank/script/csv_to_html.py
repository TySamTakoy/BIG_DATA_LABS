import pandas as pd

df = pd.read_csv('/opt/airflow/data/pagerank_results.csv')
df.to_html('/opt/airflow/data/pagerank_results.html', index=False)
