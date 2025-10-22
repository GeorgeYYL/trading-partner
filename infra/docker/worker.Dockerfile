FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir .
CMD ["python", "apps/workers/tasks/run_daily_pipeline.py"]