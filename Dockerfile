FROM python:3.12-slim

WORKDIR /app

RUN pip install pandas
COPY ingest.py ingest.py

ENTRYPOINT [ "bash" ]