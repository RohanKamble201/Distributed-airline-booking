FROM python:3.12-slim

WORKDIR /app

COPY queue_node.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "queue_node.py"]
