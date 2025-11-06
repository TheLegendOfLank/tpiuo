FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY reddit-consumer.py .
COPY private-key.json .

CMD ["python", "reddit-consumer.py"]
