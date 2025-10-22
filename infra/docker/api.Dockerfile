FROM python:3.11-slim

WORKDIR /app

# 构建期装 Poetry；运行期不用 Poetry
RUN pip install --no-cache-dir "poetry==1.8.3" && \
    poetry config virtualenvs.create false

# 先拷依赖清单（利用缓存）
COPY pyproject.toml poetry.lock* ./

# 从 pyproject 安装所有依赖（不安装项目本身）
RUN poetry install --no-interaction --no-ansi --no-root

# 再拷源码
COPY . .

# 运行期从源码启动
ENV PYTHONPATH=/app
EXPOSE 8000
CMD ["python","-m","uvicorn","apps.api.main:app","--host","0.0.0.0","--port","8000"]
