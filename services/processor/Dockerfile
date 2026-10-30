FROM python:3.12-slim
WORKDIR /app
COPY services/processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline ./pipeline
COPY services/processor/main.py .
COPY tests ./tests
CMD ["python", "main.py"]
