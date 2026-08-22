FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app/gateway

COPY gateway/pyproject.toml gateway/README.md ./
COPY gateway/origin_gateway ./origin_gateway

RUN pip install --no-cache-dir .

EXPOSE 9400
CMD ["uvicorn", "origin_gateway.main:app", "--host", "0.0.0.0", "--port", "9400"]
