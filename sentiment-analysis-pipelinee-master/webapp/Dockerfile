# webapp/Dockerfile
FROM python:3.9-slim
RUN pip install requests

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["python", "stream_to_kafka.py"]
