FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Los navegadores de Playwright se instalan en una ruta compartida (los usa el usuario sin root)
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && playwright install --with-deps chromium \
 && chmod -R a+rx /ms-playwright

COPY . .

# Seguridad: el contenedor NO corre como root
RUN useradd --create-home appuser
USER appuser

# La misma imagen sirve para: scheduler de Dask, workers de Dask y el runner del flow de Prefect
CMD ["python", "flow.py"]
