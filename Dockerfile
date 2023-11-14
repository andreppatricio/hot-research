# Use the base image as the starting point
FROM apache/airflow:2.7.2

# Install additional Python packages using pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" feedparser==6.0.10 nltk==3.8.1 langdetect==1.0.9
