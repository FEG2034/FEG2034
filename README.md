# FEG2034

《GDELT_online.py》
==================
terminal command: spark-submit GDELT_online.py YYYYMMDD YYYYMMDD event_number  
ex: spark-submit GDELT_online.py 20170303 20170303 19  
  
只要修改 Part(0) 的 global varaible，就可以用在不同的 GCP 帳戶上  
1. dataset_on_BigQuery = 預先在 BigQuery 的 project 之下，建立的 dataset 名稱  
2. table_name = 用 query 當下的時間戳記，命名 table  
3. partition_number = 把 RDD 做切割，再算做 PageRank
