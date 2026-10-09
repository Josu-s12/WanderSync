FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Seguridad: el contenedor NO corre como root
RUN useradd --create-home appuser
USER appuser

# La misma imagen sirve para: scheduler de Dask, workers de Dask y el runner del flow de Prefect
CMD ["python", "flow.py"]
