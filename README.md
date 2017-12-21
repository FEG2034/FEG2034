# FEG2034

《GDELT_online.py》
只要修改 Part(0) 的 global varaible，就可以用在不同的 GCP 帳戶上

dataset_on_BigQuery = 預先在 BigQuery 的 project 之下，建立的 dataset 名稱
table_name = 用 query 當下的時間戳記，命名 table
partition_number = 把 RDD 做切割，再算做 PageRank
