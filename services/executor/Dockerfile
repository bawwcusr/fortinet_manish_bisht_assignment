FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev || uv sync --no-dev
COPY . .
RUN chmod +x entrypoint.sh
EXPOSE 8081
CMD ["uv", "run", "./entrypoint.sh"]
