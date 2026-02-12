FROM python:3.12

WORKDIR /app
COPY . .
ENTRYPOINT ["python", "main.py"]