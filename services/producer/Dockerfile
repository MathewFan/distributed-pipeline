FROM python:3.12-slim
WORKDIR /app
COPY services/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline ./pipeline
COPY services/producer/app.py .
COPY tests ./tests
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
