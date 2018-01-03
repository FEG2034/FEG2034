# FEG2034

《GDELT_PageRank.py》
==================
terminal command: spark-submit GDELT_online.py YYYYMMDD YYYYMMDD event_number  
ex: spark-submit GDELT_online.py 20170303 20170303 19  
  
只要修改變數：DATASET_BigQuery，就可以用在不同的 GCP 帳戶上使用
---
About DSFinalGUI
* ### 1. Website based on Django 1.11 and Python 2.7
* ### 2. How to start server
*   ### Go to DSFinalGUI directory
*   ### python manage.py runserver 0.0.0.0:YOUR_SPECIFIED_PORT_NUMBER <--- Should also be allowed in GCP firewall. See your GCP console-->VPC network-->Firewall rules
*   ### 3. Start a web browser and goto YOUR_IP_AT_GCP_HOST:YOUR_SPECIFIED_PORT/DSFinal to see if if worked. Cheers.
*   ### 4. If you want to change the GDELT_xxxx.py executables, copy your .py to DSFinalGUI/DSFinal directory and modify views.py at Line 27.
