FROM python:3.13.3

WORKDIR /app
COPY analyzer/gateway/. .
RUN pip install --no-cache-dir -r requirements.txt
ENTRYPOINT ["python3", "-u", "main.py"]
